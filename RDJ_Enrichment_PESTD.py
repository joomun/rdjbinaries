import os
import sys
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from threading import Lock
import queue
import multiprocessing as mp
import psutil  # <-- Add this import

# --- Constants ---
DATE_LENGTH = 8
TIME_LENGTH = 6
DEFAULT_INPUT_TIME = "000000"
HEADER_LENGTH = 155
HEADER_MVT_LABEL = "INV_MARCHE"
HEADER_MVT_LABEL_LENGTH = 20
OUTPUT_FILE_EXTENSION = ".out"
REJECT_FILE_EXTENSION = ".rej"
AMOUNT_FIELD_LENGTH = 18
AMOUNT_DECIMAL_NR = "3"
SIGN_FIELD_LENGTH = 1
EMPTY_CURRENCY = "   "
TIERS_RICOS_FIELD_LENGTH = 12
INPUT_FILE_SEPARATOR = "|"


# Batch processing settings - Optimized for 8 workers
BATCH_SIZE = 80000            # 80K records per batch (divisible by 8)
CHUNK_SIZE = 10000            # 8 chunks per batch (80K / 8 workers)
BUFFER_SIZE = 4 * 1024 * 1024 # 4MB I/O buffer
REJECT_BUFFER_SIZE = 5000     # Reject messages before flush
NUM_WORKERS = 8               # Fixed 8 workers
USE_MULTIPROCESSING = True   # Use threading - avoids memory/pickle issues

# Field Types
ADD_RICOS_SC_CPY_USING_SIAM = 10
ADD_RICOS_SC_CPY_USING_RTS = 100
ADD_RICOS_SC_USING_RTS = 200

# Field Formats
FORMAT_SKIP = 0
FORMAT_CHAR_ED = 1
FORMAT_CHAR_EG = 2
FORMAT_ENTIER_EG = 3
FORMAT_ENTIER_ZG = 4
FORMAT_ENTIER_SG = 5
FORMAT_ENTIER_SZG = 6
FORMAT_ENTIER_SD = 7
FORMAT_ENTIER_SZD = 8
FORMAT_AMOUNT_3DEC = 9
FORMAT_UNKNOWN = -1

FORMAT_MAP = {
    "skip": FORMAT_SKIP,
    "charED": FORMAT_CHAR_ED,
    "charEG": FORMAT_CHAR_EG,
    "entierEG": FORMAT_ENTIER_EG,
    "entierZG": FORMAT_ENTIER_ZG,
    "entierSG": FORMAT_ENTIER_SG,
    "entierSZG": FORMAT_ENTIER_SZG,
    "entierSD": FORMAT_ENTIER_SD,
    "entierSZD": FORMAT_ENTIER_SZD,
    "amount3DEC": FORMAT_AMOUNT_3DEC,
}

TIERS_RTS_SC_FIELDS = frozenset({
    "Z_ALIAS_ID_EMPPRET", "Z_ALIAS_ID_GARANT", "Z_ALIAS_ID_EMETTIT",
    "Z_ALIAS_ID_DEPOSIT", "Z_ALIAS_ID_EMETSSJ", "Z_ALIAS_ID_ACTR", "Z_ALIAS_ID_TIERORI"
})



# --- Worker Process Global State ---
# These are initialized in each worker process via initializer
_worker_siam_table: Dict[str, Tuple[bytes, bytes]] = {}
_worker_rts_table: Dict[str, Tuple[bytes, bytes]] = {}
_worker_currency_table: Dict[str, str] = {}
_worker_field_defs: List = []
_worker_special_indices: Dict[str, int] = {}
_worker_initialized = False

def _init_worker(siam_data: Dict, rts_data: Dict, currency_data: Dict, 
                 field_data: List, special_data: Dict) -> None:
    """Initialize worker process with shared data structures"""
    global _worker_siam_table, _worker_rts_table, _worker_currency_table
    global _worker_field_defs, _worker_special_indices, _worker_initialized
    
    _worker_siam_table = siam_data
    _worker_rts_table = rts_data
    _worker_currency_table = currency_data
    _worker_field_defs = field_data
    _worker_special_indices = special_data
    _worker_initialized = True
    
    # Log worker initialization with PID
    print(f"WORKER [{get_timestamp()}] : Worker process {os.getpid()} initialized with {len(siam_data)} SIAM, {len(rts_data)} RTS, {len(currency_data)} currencies, {len(field_data)} fields")

# --- Thread-Safe Buffered Logger ---
class Logger:
    __slots__ = ('reject_buffer', 'reject_file', 'reject_count', 'buffer_size', '_lock')
    
    def __init__(self):
        self.reject_buffer: List[str] = []
        self.reject_file = None
        self.reject_count = 0
        self.buffer_size = REJECT_BUFFER_SIZE
        self._lock = Lock()
    
    def open_reject_file(self, input_file: str) -> None:
        reject_path = input_file + REJECT_FILE_EXTENSION
        self.reject_file = open(reject_path, "w", encoding="utf-8", buffering=BUFFER_SIZE)
        log_process(f"Opening Reject File : {reject_path}")
    
    def log_reject(self, msg: str) -> None:
        with self._lock:
            self.reject_count += 1
            self.reject_buffer.append(msg)
            if len(self.reject_buffer) >= self.buffer_size:
                self._flush_rejects_unsafe()
    
    def log_rejects_batch(self, messages: List[str]) -> None:
        """Add multiple reject messages at once - thread-safe"""
        if not messages:
            return
        with self._lock:
            self.reject_count += len(messages)
            self.reject_buffer.extend(messages)
            if len(self.reject_buffer) >= self.buffer_size:
                self._flush_rejects_unsafe()
    
    def _flush_rejects_unsafe(self) -> None:
        """Internal flush - must be called with lock held"""
        if self.reject_file and self.reject_buffer:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.reject_file.write('\n'.join(f"[{ts}] {m}" for m in self.reject_buffer) + '\n')
            self.reject_buffer.clear()
    
    def _flush_rejects(self) -> None:
        with self._lock:
            self._flush_rejects_unsafe()
    
    def close(self) -> None:
        self._flush_rejects()
        if self.reject_file:
            self.reject_file.close()
            self.reject_file = None

logger = Logger()

def get_timestamp() -> str:
    now = datetime.now()
    return f"{now.strftime('%Y-%m-%d %H:%M:%S')}.{now.microsecond // 1000:03d}"

def log_process(msg: str) -> None:
    print(f"PROCESS [{get_timestamp()}] : {msg}")

def print_ram_usage(note: str = "") -> None:
    """Print current RAM usage in MB"""
    try:
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        rss_mb = mem_info.rss / (1024 * 1024)
        vms_mb = mem_info.vms / (1024 * 1024)
        
        # Get system memory info
        system_mem = psutil.virtual_memory()
        system_used_mb = system_mem.used / (1024 * 1024)
        system_total_mb = system_mem.total / (1024 * 1024)
        
        note_str = f" ({note})" if note else ""
        log_process(f"RAM Usage{note_str}: Process={rss_mb:.1f}MB (Virtual={vms_mb:.1f}MB) | System={system_used_mb:.0f}MB/{system_total_mb:.0f}MB ({system_mem.percent:.1f}%)")
    except Exception as e:
        log_process(f"Error reading RAM usage: {e}")

# --- Optimized Utility Functions ---
def is_leap_year(year: int) -> bool:
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)

def is_valid_date(date_str: str) -> bool:
    if len(date_str) != DATE_LENGTH or not date_str.isdigit():
        return False
    year, month, day = int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8])
    if month < 1 or month > 12:
        return False
    days = (0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
    max_day = days[month] + (1 if month == 2 and is_leap_year(year) else 0)
    return 1 <= day <= max_day

def is_valid_time(time_str: str) -> bool:
    if len(time_str) != TIME_LENGTH or not time_str.isdigit():
        return False
    h, m, s = int(time_str[:2]), int(time_str[2:4]), int(time_str[4:6])
    return 0 <= h <= 23 and 0 <= m <= 59 and 0 <= s <= 59

def parse_numeric(field: str) -> Tuple[bool, str, int]:
    """Parse numeric field - returns (is_valid, value, sign)"""
    if not field:
        return False, field, 0
    
    sign = 1
    s = field
    
    c = s[0]
    if c == '-':
        sign, s = -1, s[1:]
    elif c == '+':
        s = s[1:]
    elif len(s) > 1:
        c = s[-1]
        if c == '-':
            sign, s = -1, s[:-1]
        elif c == '+':
            s = s[:-1]
    
    s = s.strip()
    if s and s.isdigit():
        i = 0
        while i < len(s) - 1 and s[i] == '0':
            i += 1
        return True, s[i:], sign
    return False, field, 0

def parse_amount(amount_str: str) -> Tuple[bool, str, int, int]:
    """Parse amount - returns (is_valid, value, sign, decimals)"""
    dot_pos = amount_str.rfind('.')
    decimal_count = 0
    
    if dot_pos != -1:
        end = len(amount_str)
        for i in range(dot_pos + 1, len(amount_str)):
            if amount_str[i] == ' ':
                end = i
                break
        decimal_count = end - dot_pos - 1
        s = amount_str[:dot_pos] + amount_str[dot_pos + 1:]
    else:
        s = amount_str
    
    ok, val, sign = parse_numeric(s)
    return (True, val, sign, decimal_count) if ok else (False, amount_str, 0, 0)

def correct_format_amount(amount: str, cur_dec_str: str) -> str:
    """Correct amount format based on currency decimals"""
    cur_dec = int(cur_dec_str)
    amt_dec = int(amount[-1])
    sign = amount[0]
    
    if cur_dec == amt_dec:
        shift = 3 - cur_dec
        result = sign + amount[1 + shift:1 + AMOUNT_FIELD_LENGTH] + '0' * shift + AMOUNT_DECIMAL_NR
    elif cur_dec > amt_dec:
        diff = cur_dec - amt_dec
        result = sign + amount[1 + diff:1 + AMOUNT_FIELD_LENGTH] + '0' * diff + AMOUNT_DECIMAL_NR
    else:
        diff = amt_dec - cur_dec
        exp = 3 - cur_dec
        # Apply rounding like C code: ROUND(x) = floor(x) if (x - floor(x)) < 0.5 else ceil(x)
        raw_val = int(amount[1:1 + AMOUNT_FIELD_LENGTH])
        divisor = 10 ** diff
        # Standard rounding (round half up)
        val = (raw_val + divisor // 2) // divisor
        result = sign + f"{val:017d}"[exp:] + '0' * exp + AMOUNT_DECIMAL_NR
    
    if len(result) < 20:
        result = result[0] + result[1:-1].zfill(AMOUNT_FIELD_LENGTH) + result[-1]
    return result[:20]

# --- Serializable Field Definition for multiprocessing ---
class FieldDefData:
    """Serializable field definition for passing to worker processes"""
    __slots__ = ('name', 'format_type', 'field_type', 'length_output',
                 'start_pos_output', 'input_field_index', 'hash_fill', 'is_special')
    
    def __init__(self):
        self.name = ""
        self.format_type = FORMAT_UNKNOWN
        self.field_type = -1
        self.length_output = 0
        self.start_pos_output = 0
        self.input_field_index = -1
        self.hash_fill = b''
        self.is_special = False

# Global state for main process - read-only after initialization
siam_table: Dict[str, Tuple[bytes, bytes]] = {}
rts_table: Dict[str, Tuple[bytes, bytes]] = {}
currency_table: Dict[str, str] = {}
field_defs: List[FieldDefData] = []
output_record_size = HEADER_LENGTH
special_indices: Dict[str, int] = {}

# Pre-computed values for hot path
_default_time_bytes = DEFAULT_INPUT_TIME.encode('latin1')
_hash_fill_12 = b'#' * TIERS_RICOS_FIELD_LENGTH
_newline_bytes = b'\n'

# --- Converter functions ---
def convert_char_ed(s: str, length: int) -> bytes:
    return s.strip().ljust(length).encode('latin1')

def convert_char_eg(s: str, length: int) -> bytes:
    return s.strip().rjust(length).encode('latin1')

def convert_entier_eg(s: str, length: int) -> bytes:
    ok, val, _ = parse_numeric(s)
    return val.rjust(length).encode('latin1') if ok else s[:length].encode('latin1')

def convert_entier_zg(s: str, length: int) -> bytes:
    ok, val, _ = parse_numeric(s)
    return val.zfill(length).encode('latin1') if ok else s[:length].encode('latin1')

def convert_entier_sg(s: str, length: int) -> bytes:
    ok, val, sign = parse_numeric(s)
    if ok:
        return (('-' if sign < 0 else '+') + val.rjust(length - 1)).encode('latin1')
    return s[:length].encode('latin1')

def convert_entier_szg(s: str, length: int) -> bytes:
    ok, val, sign = parse_numeric(s)
    if ok:
        return (('-' if sign < 0 else '+') + val.zfill(length - 1)).encode('latin1')
    return s[:length].encode('latin1')

def convert_entier_sd(s: str, length: int) -> bytes:
    ok, val, sign = parse_numeric(s)
    if ok:
        return (val.rjust(length - 1) + ('-' if sign < 0 else '+')).encode('latin1')
    return s[:length].encode('latin1')

def convert_entier_szd(s: str, length: int) -> bytes:
    ok, val, sign = parse_numeric(s)
    if ok:
        return (val.zfill(length - 1) + ('-' if sign < 0 else '+')).encode('latin1')
    return s[:length].encode('latin1')

def convert_amount(s: str, length: int) -> bytes:
    ok, val, sign, dec = parse_amount(s)
    if ok:
        result = ('-' if sign < 0 else '+') + val.zfill(length - 2) + str(dec)
        return result[:length].encode('latin1')
    return s[:length].encode('latin1')

def convert_skip(s: str, length: int) -> bytes:
    return s[:length].encode('latin1') if length > 0 else s.encode('latin1')

# Converter dispatch table
CONVERTERS = {
    FORMAT_SKIP: convert_skip,
    FORMAT_CHAR_ED: convert_char_ed,
    FORMAT_CHAR_EG: convert_char_eg,
    FORMAT_ENTIER_EG: convert_entier_eg,
    FORMAT_ENTIER_ZG: convert_entier_zg,
    FORMAT_ENTIER_SG: convert_entier_sg,
    FORMAT_ENTIER_SZG: convert_entier_szg,
    FORMAT_ENTIER_SD: convert_entier_sd,
    FORMAT_ENTIER_SZD: convert_entier_szd,
    FORMAT_AMOUNT_3DEC: convert_amount,
}

def convert_field(s: str, format_type: int, length: int) -> bytes:
    """Convert a field value based on format type and length"""
    converter = CONVERTERS.get(format_type, convert_skip)
    return converter(s, length)

_char_ed_12 = lambda s: convert_char_ed(s, TIERS_RICOS_FIELD_LENGTH)
_entier_zg_12 = lambda s: convert_entier_zg(s, TIERS_RICOS_FIELD_LENGTH)

def build_ref_tiers_tables(conf_dir: str) -> None:
    global siam_table, rts_table
    ref_tiers_file = os.path.join(conf_dir, "REF_TIERS.dat")
    log_process(f"Opening {ref_tiers_file} File ...")
    print_ram_usage("before tiers load")  # <-- Add RAM check
    
    local_siam: Dict[str, Tuple[bytes, bytes]] = {}
    local_rts: Dict[str, Tuple[bytes, bytes]] = {}
    reject_messages: List[str] = []
    
    with open(ref_tiers_file, encoding="latin1", buffering=BUFFER_SIZE) as f:
        for rec_num, line in enumerate(f):
            if len(line) <= 1:
                continue
            parts = line.rstrip('\n\r').split(';')
            if len(parts) != 6:
                reject_messages.append(f"[BuildTiersRicosTables] - Invalid separator count in Record {rec_num:06d}")
                continue
            
            siam, ricos_sc, ricos_cpy, rts_sc = parts[0].strip(), parts[1].strip(), parts[2].strip(), parts[3].strip()
            
            if not siam and not rts_sc:
                reject_messages.append(f"[BuildTiersRicosTables] - SIAM and RTS empty in Record {rec_num:06d}")
                continue
            
            sc_bytes = _char_ed_12(ricos_sc)
            cpy_bytes = _entier_zg_12(ricos_cpy)
            
            if siam:
                local_siam[siam + ";"] = (sc_bytes, cpy_bytes)
            if rts_sc:
                local_rts[rts_sc] = (sc_bytes, cpy_bytes)
    
    siam_table.clear()
    siam_table.update(local_siam)
    rts_table.clear()
    rts_table.update(local_rts)
    
    if reject_messages:
        logger.log_rejects_batch(reject_messages)
    
    print_ram_usage("after tiers load")  # <-- Add RAM check
    log_process(f"Loaded {len(siam_table)} SIAM entries, {len(rts_table)} RTS entries")
    log_process(f"Closing {ref_tiers_file} File ...")

def build_currency_table(conf_dir: str) -> None:
    global currency_table
    ref_currency_file = os.path.join(conf_dir, "REF_CURRENCY.dat")
    log_process(f"Opening {ref_currency_file} File ...")
    print_ram_usage("before currency load")  # <-- Add RAM check
    
    local_currency: Dict[str, str] = {}
    
    with open(ref_currency_file, encoding="latin1", buffering=BUFFER_SIZE) as f:
        for line in f:
            if len(line) > 1:
                parts = line.rstrip('\n\r').split(';')
                if len(parts) >= 2:
                    cur, dec = parts[0].strip(), parts[1].strip()
                    if cur and dec.isdigit():
                        local_currency[cur] = dec
    
    currency_table.clear()
    currency_table.update(local_currency)
    
    print_ram_usage("after currency load")  # <-- Add RAM check
    log_process(f"Loaded {len(currency_table)} currencies")
    log_process(f"Closing {ref_currency_file} File ...")

def build_output_record_format(conf_dir: str) -> None:
    global field_defs, output_record_size, special_indices
    struct_file = os.path.join(conf_dir, "struct_pestd.conf")
    log_process(f"Opening {struct_file} File ...")
    print_ram_usage("before struct load")  # <-- Add RAM check
    
    local_field_defs: List[FieldDefData] = []
    local_special_indices: Dict[str, int] = {}
    start_pos = HEADER_LENGTH
    input_idx = 0
    
    special_names = frozenset({"EMISS_CRS", "CODE_DEVISE_ISO", "QTE_DECIMALES", 
                               "I_SIGN_MNT_DEVISE", "Z_MNT_ESTD_DEVISE"})
    
    with open(struct_file, encoding="latin1") as f:
        for line in f:
            line = line.rstrip('\n\r')
            if len(line.strip()) <= 1:
                continue
            
            parts = line.split(';')
            field = FieldDefData()
            field.name = parts[0]
            field.format_type = FORMAT_MAP.get(parts[1] if len(parts) > 1 else "skip", FORMAT_UNKNOWN)
            field.length_output = int(parts[2]) if len(parts) > 2 else 0
            field.start_pos_output = start_pos
            field.input_field_index = input_idx
            field.is_special = field.name in special_names
            input_idx += 1
            
            if field.is_special:
                local_special_indices[field.name] = len(local_field_defs)
            
            if field.name == "TIERS":
                field.field_type = ADD_RICOS_SC_CPY_USING_SIAM
                local_field_defs.append(field)
                start_pos += field.length_output
                _add_ricos_pair_local(local_field_defs, start_pos)
                start_pos = local_field_defs[-1].start_pos_output + local_field_defs[-1].length_output
            elif field.name == "Z_ALIAS_ID_TIERS":
                field.field_type = ADD_RICOS_SC_CPY_USING_RTS
                local_field_defs.append(field)
                start_pos += field.length_output
                _add_ricos_pair_local(local_field_defs, start_pos)
                start_pos = local_field_defs[-1].start_pos_output + local_field_defs[-1].length_output
            elif field.name in TIERS_RTS_SC_FIELDS:
                field.field_type = ADD_RICOS_SC_USING_RTS
                local_field_defs.append(field)
                start_pos += field.length_output
                _add_ricos_single_local(local_field_defs, start_pos)
                start_pos = local_field_defs[-1].start_pos_output + local_field_defs[-1].length_output
            else:
                local_field_defs.append(field)
                start_pos += field.length_output
    
    for fd in local_field_defs:
        fd.hash_fill = b'#' * fd.length_output
    
    field_defs.clear()
    field_defs.extend(local_field_defs)
    special_indices.clear()
    special_indices.update(local_special_indices)
    output_record_size = start_pos
    
    print_ram_usage("after struct load")  # <-- Add RAM check
    log_process(f"Output record size: {output_record_size} bytes, {len(field_defs)} fields")
    log_process(f"Closing {struct_file} File ...")

def _add_ricos_pair_local(local_defs: List[FieldDefData], start_pos: int) -> None:
    f1 = FieldDefData()
    f1.format_type = FORMAT_CHAR_ED
    f1.length_output = TIERS_RICOS_FIELD_LENGTH
    f1.start_pos_output = start_pos
    f1.input_field_index = -1
    f1.hash_fill = _hash_fill_12
    local_defs.append(f1)
    
    f2 = FieldDefData()
    f2.format_type = FORMAT_ENTIER_ZG
    f2.length_output = TIERS_RICOS_FIELD_LENGTH
    f2.start_pos_output = start_pos + TIERS_RICOS_FIELD_LENGTH
    f2.input_field_index = -1
    f2.hash_fill = _hash_fill_12
    local_defs.append(f2)

def _add_ricos_single_local(local_defs: List[FieldDefData], start_pos: int) -> None:
    f = FieldDefData()
    f.format_type = FORMAT_CHAR_ED
    f.length_output = TIERS_RICOS_FIELD_LENGTH
    f.start_pos_output = start_pos
    f.input_field_index = -1
    f.hash_fill = _hash_fill_12
    local_defs.append(f)

def process_record_worker(input_fields: List[str], output: bytearray, num_input: int, 
                          local_rejects: List[str], w_field_defs: List, 
                          w_siam: Dict, w_rts: Dict, w_currency: Dict) -> None:
    """Process a single record using worker-local data structures (C-like robustness)"""
    devise_decimal = "3"
    sign_val = ""
    amount_val = ""
    decimal_val = ""

    idx = 0
    num_fields = len(w_field_defs)

    while idx < num_fields:
        fd = w_field_defs[idx]
        inp_idx = fd.input_field_index

        # Defensive: always fill with hash if missing
        if 0 <= inp_idx < num_input:
            input_val = input_fields[inp_idx]
        else:
            input_val = ""
            local_rejects.append(f"Field {fd.name} missing in input")
            out_bytes = fd.hash_fill
            output[fd.start_pos_output:fd.start_pos_output + fd.length_output] = out_bytes
            idx += 1
            continue

        # Try conversion, always fill output
        try:
            out_bytes = convert_field(input_val, fd.format_type, fd.length_output)
        except Exception as e:
            local_rejects.append(f"Field {fd.name} conversion error: {e}")
            out_bytes = fd.hash_fill

        name = fd.name
        if fd.is_special:
            if name == "EMISS_CRS":
                if len(out_bytes) >= DATE_LENGTH + TIME_LENGTH:
                    time_part = out_bytes[DATE_LENGTH:DATE_LENGTH + TIME_LENGTH].decode('latin1')
                    if not is_valid_time(time_part):
                        out_bytes = out_bytes[:DATE_LENGTH] + _default_time_bytes + out_bytes[DATE_LENGTH + TIME_LENGTH:]
            elif name == "CODE_DEVISE_ISO":
                devise = input_val.strip()
                if devise and devise != EMPTY_CURRENCY:
                    devise_decimal = w_currency.get(devise, "3")
                    if devise not in w_currency:
                        local_rejects.append(f"Currency NOT FOUND: {devise}")
                else:
                    devise_decimal = "3"
            elif name == "QTE_DECIMALES":
                decimal_val = input_val
                out_bytes = AMOUNT_DECIMAL_NR.ljust(fd.length_output).encode('latin1')
            elif name == "I_SIGN_MNT_DEVISE":
                sign_val = out_bytes.decode('latin1')
            elif name == "Z_MNT_ESTD_DEVISE":
                amount_val = out_bytes.decode('latin1')
                if sign_val and amount_val:
                    combined = sign_val + amount_val + decimal_val
                    if len(combined) >= SIGN_FIELD_LENGTH + AMOUNT_FIELD_LENGTH + 1:
                        corrected = correct_format_amount(combined, devise_decimal)
                        out_bytes = corrected[SIGN_FIELD_LENGTH:SIGN_FIELD_LENGTH + fd.length_output].encode('latin1')

        # Always fill output, even if empty or error
        start = fd.start_pos_output
        output[start:start + fd.length_output] = out_bytes

        ft = fd.field_type
        if ft == ADD_RICOS_SC_CPY_USING_SIAM:
            trimmed = input_val.strip()
            entry = w_siam.get(trimmed + ";") if trimmed else None
            idx += 1
            fd1 = w_field_defs[idx]
            idx += 1
            fd2 = w_field_defs[idx]
            if entry:
                output[fd1.start_pos_output:fd1.start_pos_output + TIERS_RICOS_FIELD_LENGTH] = entry[0]
                output[fd2.start_pos_output:fd2.start_pos_output + TIERS_RICOS_FIELD_LENGTH] = entry[1]
            else:
                if trimmed:
                    local_rejects.append(f"SIAM Key {trimmed}; NOT FOUND")
                output[fd1.start_pos_output:fd1.start_pos_output + TIERS_RICOS_FIELD_LENGTH] = fd1.hash_fill
                output[fd2.start_pos_output:fd2.start_pos_output + TIERS_RICOS_FIELD_LENGTH] = fd2.hash_fill

        elif ft == ADD_RICOS_SC_CPY_USING_RTS:
            trimmed = input_val.strip()
            entry = w_rts.get(trimmed) if trimmed else None
            idx += 1
            fd1 = w_field_defs[idx]
            idx += 1
            fd2 = w_field_defs[idx]
            if entry:
                output[fd1.start_pos_output:fd1.start_pos_output + TIERS_RICOS_FIELD_LENGTH] = entry[0]
                output[fd2.start_pos_output:fd2.start_pos_output + TIERS_RICOS_FIELD_LENGTH] = entry[1]
            else:
                if trimmed:
                    local_rejects.append(f"RTS Key {trimmed} NOT FOUND")
                output[fd1.start_pos_output:fd1.start_pos_output + TIERS_RICOS_FIELD_LENGTH] = fd1.hash_fill
                output[fd2.start_pos_output:fd2.start_pos_output + TIERS_RICOS_FIELD_LENGTH] = fd2.hash_fill

        elif ft == ADD_RICOS_SC_USING_RTS:
            trimmed = input_val.strip()
            entry = w_rts.get(trimmed) if trimmed else None
            idx += 1
            fd1 = w_field_defs[idx]
            if entry:
                output[fd1.start_pos_output:fd1.start_pos_output + TIERS_RICOS_FIELD_LENGTH] = entry[0]
            else:
                if trimmed:
                    local_rejects.append(f"RTS Key {trimmed} NOT FOUND")
                output[fd1.start_pos_output:fd1.start_pos_output + TIERS_RICOS_FIELD_LENGTH] = fd1.hash_fill

        idx += 1

def process_chunk_worker(chunk_data: Tuple[int, List[str], bytes, int]) -> Tuple[int, bytearray, List[str], int]:
    """Process a chunk of lines in worker process - returns chunk_id for ordering"""
    chunk_id, lines, template_bytes, record_size = chunk_data
    
    # Use worker-local data
    w_field_defs = _worker_field_defs
    w_siam = _worker_siam_table
    w_rts = _worker_rts_table
    w_currency = _worker_currency_table
    
    local_rejects: List[str] = []
    count = 0
    
    # Pre-allocate output buffer
    output_buffer = bytearray(len(lines) * (record_size + 1))
    write_pos = 0
    
    for line in lines:
        if len(line) <= 2:
            continue
        
        count += 1
        output = bytearray(template_bytes)
        input_fields = line.rstrip('\n\r').split(INPUT_FILE_SEPARATOR)
        process_record_worker(input_fields, output, len(input_fields), local_rejects,
                              w_field_defs, w_siam, w_rts, w_currency)
        
        end_pos = write_pos + record_size
        output_buffer[write_pos:end_pos] = output
        output_buffer[end_pos] = ord('\n')
        write_pos = end_pos + 1
    
    # Return chunk_id to maintain order
    return chunk_id, output_buffer[:write_pos], local_rejects, count

def process_chunk_thread(chunk_data: Tuple[int, List[str], bytes, int]) -> Tuple[int, bytearray, List[str], int]:
    """Process a chunk of lines using main process globals (for ThreadPoolExecutor)"""
    chunk_id, lines, template_bytes, record_size = chunk_data
    
    local_rejects: List[str] = []
    count = 0
    
    output_buffer = bytearray(len(lines) * (record_size + 1))
    write_pos = 0
    
    for line in lines:
        if len(line) <= 2:
            continue
        
        count += 1
        output = bytearray(template_bytes)
        input_fields = line.rstrip('\n\r').split(INPUT_FILE_SEPARATOR)
        process_record_worker(input_fields, output, len(input_fields), local_rejects,
                              field_defs, siam_table, rts_table, currency_table)
        
        end_pos = write_pos + record_size
        output_buffer[write_pos:end_pos] = output
        output_buffer[end_pos] = ord('\n')
        write_pos = end_pos + 1
    
    return chunk_id, output_buffer[:write_pos], local_rejects, count

def process_batch_parallel_ordered(lines: List[str], template_bytes: bytes, record_size: int, 
                                   executor, use_multiprocessing: bool) -> Tuple[bytearray, List[str], int]:
    """Process a batch with guaranteed order preservation"""
    if len(lines) < CHUNK_SIZE * 2:
        # Small batch - process directly using THREAD function (uses main process globals)
        # Do NOT use process_chunk_worker here as it needs worker initialization
        log_process(f"Small batch ({len(lines):,} lines < {CHUNK_SIZE * 2:,}) - using single-threaded processing (main process PID: {os.getpid()})")
        _, output, rejects, count = process_chunk_thread((0, lines, template_bytes, record_size))
        log_process(f"Single-threaded processing complete: {count:,} records processed")
        return output, rejects, count
    
    # Large batch - use parallel processing
    num_chunks = (len(lines) + CHUNK_SIZE - 1) // CHUNK_SIZE
    log_process(f"Large batch ({len(lines):,} lines) - using {'ProcessPool' if use_multiprocessing else 'ThreadPool'} with {NUM_WORKERS} workers, {num_chunks} chunks")
    
    # Split into chunks with IDs for ordering
    chunks = []
    chunk_id = 0
    for i in range(0, len(lines), CHUNK_SIZE):
        chunk_lines = lines[i:i + CHUNK_SIZE]
        chunks.append((chunk_id, chunk_lines, template_bytes, record_size))
        chunk_id += 1
    
    # Choose the right processor function
    processor = process_chunk_worker if use_multiprocessing else process_chunk_thread
    
    # Submit all chunks
    futures = {executor.submit(processor, chunk): chunk[0] for chunk in chunks}
    
    # Collect results - store by chunk_id for ordering
    results: Dict[int, Tuple[bytearray, List[str], int]] = {}
    
    for future in as_completed(futures):
        cid, output_buffer, rejects, count = future.result()
        results[cid] = (output_buffer, rejects, count)
    
    # Reassemble in order
    all_output = bytearray()
    all_rejects: List[str] = []
    total_count = 0
    
    for cid in range(len(chunks)):
        output_buffer, rejects, count = results[cid]
        all_output.extend(output_buffer)
        all_rejects.extend(rejects)
        total_count += count
    
    log_process(f"Parallel processing complete: {total_count:,} records processed across {num_chunks} chunks")
    return all_output, all_rejects, total_count

def main() -> int:
    log_process("Start Har_Transco_PESTD Program ...")
    print_ram_usage("program start")  # <-- Add RAM check at start
    log_process(f"Workers: {NUM_WORKERS}, Batch Size: {BATCH_SIZE:,}, Chunk Size: {CHUNK_SIZE:,}")
    
    if len(sys.argv) != 3:
        log_process(f"Bad Number of Parameters: {len(sys.argv) - 1} instead of 2")
        log_process("Usage: <Input File> <Accounting Date YYYYMMDD>")
        return 1
    
    input_file, accounting_date = sys.argv[1], sys.argv[2]
    
    log_process(f"Input File : {input_file}")
    log_process(f"Accounting Date : {accounting_date}")
    
    if not is_valid_date(accounting_date):
        log_process("Invalid Accounting Date format")
        return 1
    
    conf_dir = os.environ.get("RDJ_DAT")
    if not conf_dir:
        log_process("RDJ_DAT environment variable not defined")
        return 1
    
    logger.open_reject_file(input_file)
    
    try:
        build_output_record_format(conf_dir)
        
        ref_tiers_conf = os.path.join(conf_dir, "REF_TIERS.conf")
        with open(ref_tiers_conf, encoding="latin1") as f:
            tiers_format = [l.strip() for l in f if len(l.strip()) > 1]
        expected = "SIAM;RICOS_SC_ID;RICOS_CPY_ID;RTS_SC_ID;SC_INTITULE_USUEL;SIA_CIT_TYPE"
        if ";".join(tiers_format) != expected:
            log_process("Unexpected REF_TIERS.conf format")
            return 1
        
        build_ref_tiers_tables(conf_dir)
        build_currency_table(conf_dir)
        
        print_ram_usage("after all tables loaded")  # <-- Add RAM check after loading
        
        output_file = input_file + OUTPUT_FILE_EXTENSION
        log_process(f"Processing {input_file} -> {output_file}")
        
        header = (HEADER_MVT_LABEL.ljust(HEADER_MVT_LABEL_LENGTH) + accounting_date).ljust(HEADER_LENGTH)
        header_bytes = header.encode('latin1')
        
        template_record = bytearray(b' ' * output_record_size)
        template_record[:HEADER_LENGTH] = header_bytes
        template_bytes = bytes(template_record)
        
        record_count = 0
        empty_count = 0
        batch_lines: List[str] = []
        start_time = datetime.now()
        last_ram_check = 0
        
        # Choose executor type and set up accordingly
        if USE_MULTIPROCESSING:
            log_process(f"Using ProcessPool with {NUM_WORKERS} workers")
            executor = ProcessPoolExecutor(
                max_workers=NUM_WORKERS,
                initializer=_init_worker,
                initargs=(dict(siam_table), dict(rts_table), dict(currency_table), 
                          list(field_defs), dict(special_indices))
            )
        else:
            log_process(f"Using ThreadPool with {NUM_WORKERS} workers")
            executor = ThreadPoolExecutor(max_workers=NUM_WORKERS)
        
        try:
            with open(input_file, encoding="utf-8", buffering=BUFFER_SIZE) as fin, \
                 open(output_file, "wb", buffering=BUFFER_SIZE) as fout:
                
                for line in fin:
                    if len(line) <= 2:
                        empty_count += 1
                        continue
                    
                    batch_lines.append(line)
                    
                    if len(batch_lines) >= BATCH_SIZE:
                        output_buffer, rejects, count = process_batch_parallel_ordered(
                            batch_lines, template_bytes, output_record_size, 
                            executor, USE_MULTIPROCESSING
                        )
                        fout.write(output_buffer)
                        
                        if rejects:
                            logger.log_rejects_batch(rejects)
                        
                        record_count += count
                        batch_lines = []
                        
                        if record_count % 100000 == 0:
                            elapsed = (datetime.now() - start_time).total_seconds()
                            rate = record_count / elapsed if elapsed > 0 else 0
                            log_process(f"Processed {record_count:,} records... ({rate:,.0f} rec/sec)")
                            last_ram_check = record_count
                            print_ram_usage(f"batch {record_count:,}")  # <-- Add RAM check during processing
                
                # Process remaining lines
                if batch_lines:
                    output_buffer, rejects, count = process_batch_parallel_ordered(
                        batch_lines, template_bytes, output_record_size,
                        executor, USE_MULTIPROCESSING
                    )
                    fout.write(output_buffer)
                    
                    if rejects:
                        logger.log_rejects_batch(rejects)
                    
                    record_count += count
        finally:
            executor.shutdown(wait=True)
        
        elapsed = (datetime.now() - start_time).total_seconds()
        rate = record_count / elapsed if elapsed > 0 else 0
        
        print_ram_usage("after processing complete")  # <-- Add RAM check at end
        log_process(f"Total Records Read : {record_count + empty_count:,}")
        log_process(f"Total Records Processed : {record_count:,}")
        log_process(f"Total Rejects : {logger.reject_count:,}")
        log_process(f"Processing Time : {elapsed:.2f} seconds ({rate:,.0f} rec/sec)")
        log_process("End Har_Transco_PESTD Program ...")
        
    finally:
        logger.close()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())


