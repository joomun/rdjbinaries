import os
import sys
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from math import floor, ceil

# --- Constants ---
DATE_LENGTH = 8
HEADER_LENGTH = 169
HEADER_CD_CRE = "CPTA_MARCHE"
HEADER_CD_CRE_LENGTH = 20
HEADER_ID_LOT_LENGTH = 24
HEADER_ID_COMPOST_LENGTH = 25
HEADER_ID_ECRITU_LENGTH = 6
MAX_ID_ECRITU = 999999
HEADER_CH_FILLER1_LENGTH = 94
OUTPUT_FILE_EXTENSION = ".out"
REJECT_FILE_EXTENSION = ".rej"
AMOUNT_FIELD_LENGTH = 18
AMOUNT_DECIMAL_NR = "3"
SIGN_FIELD_LENGTH = 1
DECIMAL_NR_FIELD_LENGTH = 1
EMPTY_CURRENCY = "   "
CD_TYPIMP_FIELD_LENGTH = 2  # Bilan, Hors Bilan: "BR", "HB"
CD_TYPEI_FIELD_LENGTH = 1   # Externe, Interne: "E", "I", "M"
CD_TVA_APP_FIELD_LENGTH = 2 # TVA: "00", "01", "02", "03", "10", "11", "12", "13"
NUM_CRE_IN_CD_REFOPER_LENGTH = 6

# Batch processing settings
BATCH_SIZE = 80000
BUFFER_SIZE = 4 * 1024 * 1024
REJECT_BUFFER_SIZE = 5000

# Field Types
ADD_CD_TYPIMP_TYPEI_TVA = 10
UPDATE_MAI_MNT_IMP = 110
UPDATE_MAI_MNT_GES = 120
UPDATE_MAI_MNT_NOM = 130

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


# --- Helper Functions ---
def get_timestamp() -> str:
    """Get current timestamp string"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log_process(msg: str) -> None:
    """Log process message with timestamp"""
    print(f"[{get_timestamp()}] {msg}")


class Logger:
    """Thread-safe buffered logger for reject messages"""
    __slots__ = ('reject_buffer', 'reject_file', 'reject_count', 'buffer_size')
    
    def __init__(self):
        self.reject_buffer: List[str] = []
        self.reject_file = None
        self.reject_count = 0
        self.buffer_size = REJECT_BUFFER_SIZE
    
    def open_reject_file(self, input_file: str) -> None:
        reject_path = input_file + REJECT_FILE_EXTENSION
        self.reject_file = open(reject_path, "w", encoding="utf-8", buffering=BUFFER_SIZE)
        log_process(f"Opening Reject File : {reject_path}")
    
    def log_reject(self, msg: str) -> None:
        self.reject_count += 1
        self.reject_buffer.append(msg)
        if len(self.reject_buffer) >= self.buffer_size:
            self._flush_rejects()
    
    def _flush_rejects(self) -> None:
        if self.reject_file and self.reject_buffer:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.reject_file.write('\n'.join(f"[{ts}] {m}" for m in self.reject_buffer) + '\n')
            self.reject_buffer.clear()
    
    def close(self) -> None:
        self._flush_rejects()
        if self.reject_file:
            self.reject_file.close()
            self.reject_file = None


logger = Logger()


def is_valid_date(date_str: str) -> bool:
    """Validate date in YYYYMMDD format"""
    if len(date_str) != DATE_LENGTH:
        return False
    try:
        year = int(date_str[:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])
        if year < 1900 or year > 2100:
            return False
        if month < 1 or month > 12:
            return False
        if day < 1 or day > 31:
            return False
        return True
    except ValueError:
        return False


def round_custom(x: float) -> float:
    """Custom rounding function matching C ROUND macro"""
    return floor(x) if (x - floor(x)) < 0.5 else ceil(x)


def is_numeric(s: str) -> Tuple[bool, int]:
    """Check if string is numeric and return sign"""
    s_clean = s.strip()
    if not s_clean:
        return False, 0
    
    sign = 1
    start = 0
    
    if s_clean[0] in ('+', '-'):
        sign = -1 if s_clean[0] == '-' else 1
        start = 1
    
    if start >= len(s_clean):
        return False, 0
    
    for c in s_clean[start:]:
        if not c.isdigit():
            return False, 0
    
    return True, sign


def trim(s: str) -> str:
    """Trim spaces from both ends"""
    return s.strip()


def lz_trim(s: str) -> str:
    """Trim leading zeros"""
    return s.lstrip('0') or '0'


def format_field(value: str, field_format: int, field_length: int) -> bytes:
    """Format a field according to format type"""
    if field_format == FORMAT_SKIP:
        return value.encode('latin1')[:field_length].ljust(field_length)
    
    elif field_format == FORMAT_CHAR_ED:  # Right space filled
        return value.encode('latin1')[:field_length].ljust(field_length)
    
    elif field_format == FORMAT_CHAR_EG:  # Left space filled
        return value.encode('latin1')[:field_length].rjust(field_length)
    
    elif field_format == FORMAT_ENTIER_EG:  # Unsigned numeric left spaces
        return value.encode('latin1')[:field_length].rjust(field_length)
    
    elif field_format == FORMAT_ENTIER_ZG:  # Unsigned numeric left zeroes
        return value.encode('latin1')[:field_length].rjust(field_length, b'0')
    
    elif field_format == FORMAT_ENTIER_SG:  # Signed numeric left spaces, sign at left
        return value.encode('latin1')[:field_length].rjust(field_length)
    
    elif field_format == FORMAT_ENTIER_SZG:  # Signed numeric left zeroes, sign at left
        return value.encode('latin1')[:field_length].rjust(field_length, b'0')
    
    elif field_format == FORMAT_ENTIER_SD:  # Signed numeric left spaces, sign at right
        return value.encode('latin1')[:field_length].rjust(field_length)
    
    elif field_format == FORMAT_ENTIER_SZD:  # Signed numeric left zeroes, sign at right
        return value.encode('latin1')[:field_length].rjust(field_length, b'0')
    
    elif field_format == FORMAT_AMOUNT_3DEC:
        return value.encode('latin1')[:field_length].ljust(field_length)
    
    else:
        return value.encode('latin1')[:field_length].ljust(field_length)


def parse_amount(amount_str: str) -> Tuple[bool, str, int, int]:
    """
    Parse amount string and extract sign, value, and decimal places
    Returns: (success, cleaned_amount, sign, decimal_number)
    """
    work_amount = amount_str.strip()
    
    # Find last non-space position
    last_space_pos = len(work_amount)
    decimal_symbol_pos = -1
    decimal_number = 0
    
    # Find decimal symbol
    for i in range(len(work_amount) - 1, -1, -1):
        if work_amount[i] == '.':
            decimal_symbol_pos = i
            break
    
    # If decimal symbol found, calculate decimal places
    if decimal_symbol_pos != -1:
        decimal_number = last_space_pos - decimal_symbol_pos - 1
        # Remove decimal symbol
        work_amount = work_amount[:decimal_symbol_pos] + work_amount[decimal_symbol_pos + 1:]
    
    # Check if numeric
    is_num, sign = is_numeric(work_amount)
    if is_num:
        cleaned = lz_trim(work_amount.lstrip('+-'))
        return True, cleaned, sign, decimal_number
    else:
        return False, amount_str, 0, 0


def correct_format_amount(amount_str: str, cur_decimal_nr: str) -> str:
    """
    Correct amount format based on currency decimal positions
    Returns formatted amount string with sign, amount, and decimal indicator
    """
    amount_length = len(amount_str) - 1
    amt_decimal_nr = amount_str[amount_length] if amount_length >= 0 else '3'
    
    cur_dec = int(cur_decimal_nr) if cur_decimal_nr.isdigit() else 3
    amt_dec = int(amt_decimal_nr) if amt_decimal_nr.isdigit() else 3
    
    if cur_dec == amt_dec:
        # Same decimal positions
        output = bytearray(b'0' * 20)
        output[0:1] = amount_str[0:1].encode('latin1')  # Sign
        start_pos = 1 + 3 - cur_dec
        output[1:1 + AMOUNT_FIELD_LENGTH] = amount_str[start_pos:start_pos + AMOUNT_FIELD_LENGTH].encode('latin1')
        output[1 + AMOUNT_FIELD_LENGTH:1 + AMOUNT_FIELD_LENGTH + 1] = AMOUNT_DECIMAL_NR.encode('latin1')
        return output[:20].decode('latin1')
    
    elif cur_dec > amt_dec:
        # Currency has more decimals
        diff = cur_dec - amt_dec
        output = bytearray(b'0' * 20)
        output[0:1] = amount_str[0:1].encode('latin1')  # Sign
        output[1:1 + AMOUNT_FIELD_LENGTH - diff] = amount_str[1 + diff:1 + AMOUNT_FIELD_LENGTH].encode('latin1')
        output[1 + AMOUNT_FIELD_LENGTH:1 + AMOUNT_FIELD_LENGTH + 1] = AMOUNT_DECIMAL_NR.encode('latin1')
        return output[:20].decode('latin1')
    
    else:
        # Currency has fewer decimals - need to round
        diff = amt_dec - cur_dec
        exp = 3 - cur_dec  # Must have 3 decimals
        
        amount_val = amount_str[1:1 + AMOUNT_FIELD_LENGTH]
        numerator = float(amount_val)
        denominator = pow(10, diff)
        ratio = round_custom(numerator / denominator)
        
        amount_formatted = f"{ratio:018.0f}"
        
        output = bytearray(b'0' * 20)
        output[0:1] = amount_str[0:1].encode('latin1')  # Sign
        output[1:1 + AMOUNT_FIELD_LENGTH - exp] = amount_formatted[exp:AMOUNT_FIELD_LENGTH].encode('latin1')
        output[1 + AMOUNT_FIELD_LENGTH:1 + AMOUNT_FIELD_LENGTH + 1] = AMOUNT_DECIMAL_NR.encode('latin1')
        return output[:20].decode('latin1')


def build_hash_key(key: str) -> int:
    """Build hash key from string (simple hash function)"""
    hash_val = 0
    coef = 1
    
    for i in range(len(key) - 1, -1, -1):
        if len(key) - i <= 18:  # MAX_HASH_KEY_LENGTH
            char_val = ord(key[i]) - ord(' ') if ord(key[i]) >= ord(' ') else ord(key[i])
            hash_val += char_val * coef
            coef *= 10
        else:
            break
    
    return hash_val


def load_field_definitions(config_dir: str, site: str) -> Tuple[List[Dict], int]:
    """Load field definitions from struct_premai.conf"""
    config_path = os.path.join(config_dir, "struct_premai.conf")
    
    log_process(f"Opening {config_path} File ...")
    
    fields = []
    input_rec_length = 0
    output_start_pos = HEADER_LENGTH
    input_start_pos = 0
    
    with open(config_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or len(line) <= 1:
                continue
            
            parts = line.split(';')
            if len(parts) < 3:
                continue
            
            field_name = parts[0].strip()
            field_format_str = parts[1].strip()
            field_length_input = int(parts[2].strip())
            
            field_format = FORMAT_MAP.get(field_format_str, FORMAT_UNKNOWN)
            field_length_output = field_length_input
            field_type = 0
            
            # Handle amount fields
            if field_name == "MAI_MNT_IMP":
                field_type = UPDATE_MAI_MNT_IMP
                field_format = FORMAT_AMOUNT_3DEC
                field_length_output = SIGN_FIELD_LENGTH + AMOUNT_FIELD_LENGTH + DECIMAL_NR_FIELD_LENGTH
            elif field_name == "MAI_MNT_GES":
                field_type = UPDATE_MAI_MNT_GES
                field_format = FORMAT_AMOUNT_3DEC
                field_length_output = SIGN_FIELD_LENGTH + AMOUNT_FIELD_LENGTH + DECIMAL_NR_FIELD_LENGTH
            elif field_name == "MAI_MNT_NOM":
                field_type = UPDATE_MAI_MNT_NOM
                field_format = FORMAT_AMOUNT_3DEC
                field_length_output = SIGN_FIELD_LENGTH + AMOUNT_FIELD_LENGTH + DECIMAL_NR_FIELD_LENGTH
            elif field_name == "MAI_CPT_IMP":
                field_type = ADD_CD_TYPIMP_TYPEI_TVA
            
            field_def = {
                'name': field_name,
                'format': field_format,
                'format_str': field_format_str,
                'length_input': field_length_input,
                'length_output': field_length_output,
                'start_pos_input': input_start_pos,
                'start_pos_output': output_start_pos,
                'field_type': field_type
            }
            
            fields.append(field_def)
            input_rec_length += field_length_input
            input_start_pos += field_length_input
            output_start_pos += field_length_output
            
            # Add extra fields for CD_TYPIMP_TYPEI_TVA
            if field_type == ADD_CD_TYPIMP_TYPEI_TVA:
                # Add CD_TYPIMP
                fields.append({
                    'name': 'CD_TYPIMP',
                    'format': FORMAT_CHAR_ED,
                    'format_str': 'charED',
                    'length_input': 0,
                    'length_output': CD_TYPIMP_FIELD_LENGTH,
                    'start_pos_input': -10,
                    'start_pos_output': output_start_pos,
                    'field_type': 0
                })
                output_start_pos += CD_TYPIMP_FIELD_LENGTH
                
                # Add CD_TYPEI
                fields.append({
                    'name': 'CD_TYPEI',
                    'format': FORMAT_CHAR_ED,
                    'format_str': 'charED',
                    'length_input': 0,
                    'length_output': CD_TYPEI_FIELD_LENGTH,
                    'start_pos_input': -10,
                    'start_pos_output': output_start_pos,
                    'field_type': 0
                })
                output_start_pos += CD_TYPEI_FIELD_LENGTH
                
                # Add CD_TVA_APP
                fields.append({
                    'name': 'CD_TVA_APP',
                    'format': FORMAT_CHAR_ED,
                    'format_str': 'charED',
                    'length_input': 0,
                    'length_output': CD_TVA_APP_FIELD_LENGTH,
                    'start_pos_input': -10,
                    'start_pos_output': output_start_pos,
                    'field_type': 0
                })
                output_start_pos += CD_TVA_APP_FIELD_LENGTH
    
    log_process(f"Closing {config_path} File ...")
    return fields, input_rec_length + 1  # +1 for newline


def load_dodge_table(config_dir: str, site: str) -> Dict[str, Tuple[str, str, str, str]]:
    """
    Load DODGE account table from REF_RCA_CPT.dat or REF_RCA_CPT_CAS.dat
    Returns dict: compte_dodge -> (top_bilan, hb_imputation, top_int_ext, tva)
    """
    # Determine which file to use based on site
    if site == "CASA":
        data_file = "REF_RCA_CPT_CAS.dat"
    else:
        data_file = "REF_RCA_CPT.dat"
    
    config_path = os.path.join(config_dir, "REF_RCA_CPT.conf")
    data_path = os.path.join(config_dir, data_file)
    
    log_process(f"Opening {config_path} File ...")
    
    # Parse config to get field positions
    field_positions = {}
    with open(config_path, 'r', encoding='utf-8') as f:
        field_idx = 0
        for line in f:
            line = line.strip()
            if not line or len(line) <= 1:
                continue
            
            parts = line.split(';')
            if len(parts) < 3:
                continue
            
            field_name = parts[0].strip()
            if field_name in ['COMPTE_DODGE', 'TOP_BILAN', 'TOP_INT_EXT', 'TVA']:
                field_positions[field_name] = field_idx
            
            field_idx += 1
    
    log_process(f"Closing {config_path} File ...")
    log_process(f"Opening {data_path} File ...")
    
    # Load data
    dodge_table = {}
    with open(data_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or len(line) <= 1:
                continue
            
            parts = line.split(';')
            if len(parts) < 55:  # MAX_SEPARATOR_IN_REF_RCA_CPT + 1
                continue
            
            try:
                compte_dodge = parts[field_positions.get('COMPTE_DODGE', 0)].strip()
                top_bilan = parts[field_positions.get('TOP_BILAN', 1)].strip()
                top_int_ext_raw = parts[field_positions.get('TOP_INT_EXT', 2)].strip()
                tva = parts[field_positions.get('TVA', 3)].strip()
                
                if not compte_dodge:
                    continue
                
                # Map 'M' to 'I' for TOP_INT_EXT
                top_int_ext = 'I' if top_int_ext_raw == 'M' else top_int_ext_raw
                
                # Determine HB_IMPUTATION
                hb_imputation = 'HB' if top_bilan and top_bilan[0] == 'H' else 'BR'
                
                dodge_table[compte_dodge] = (top_bilan, hb_imputation, top_int_ext, tva)
            except (IndexError, ValueError):
                continue
    
    log_process(f"Closing {data_path} File ...")
    return dodge_table


def load_currency_table(config_dir: str) -> Dict[str, str]:
    """
    Load currency decimal positions from REF_CURRENCY.dat
    Returns dict: currency_code -> decimal_pos
    """
    config_path = os.path.join(config_dir, "REF_CURRENCY.conf")
    data_path = os.path.join(config_dir, "REF_CURRENCY.dat")
    
    log_process(f"Opening {config_path} File ...")
    log_process(f"Closing {config_path} File ...")
    log_process(f"Opening {data_path} File ...")
    
    currency_table = {}
    with open(data_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or len(line) <= 1:
                continue
            
            parts = line.split(';')
            if len(parts) >= 2:
                currency_cd = parts[0].strip()
                decimal_pos = parts[1].strip()
                
                if currency_cd and decimal_pos.isdigit():
                    currency_table[currency_cd] = decimal_pos
    
    log_process(f"Closing {data_path} File ...")
    return currency_table


class LOTManager:
    """Manage LOT numbers and ID_ECRITU counters"""
    
    def __init__(self):
        self.lot_table: Dict[str, Dict] = {}  # key: APPLI_EMET+ID_LOT -> {lot_num, id_ecritu}
        self.last_lot_num = 0
    
    def get_lot_and_id(self, appli_emet: str, id_lot: str) -> Tuple[int, int]:
        """Get or create LOT_NUM and ID_ECRITU for given APPLI_EMET and ID_LOT"""
        key = appli_emet + id_lot
        
        if key in self.lot_table:
            entry = self.lot_table[key]
            entry['id_ecritu'] += 1
            if entry['id_ecritu'] > MAX_ID_ECRITU:
                entry['id_ecritu'] = 1
            return entry['lot_num'], entry['id_ecritu']
        else:
            self.last_lot_num += 1
            self.lot_table[key] = {
                'lot_num': self.last_lot_num,
                'id_ecritu': 1,
                'appli_emet': appli_emet,
                'id_lot': id_lot
            }
            return self.last_lot_num, 1


def process_record(
    line: str,
    field_defs: List[Dict],
    dodge_table: Dict[str, Tuple[str, str, str, str]],
    currency_table: Dict[str, str],
    lot_manager: LOTManager,
    output_rec_size: int
) -> Optional[bytes]:
    """Process a single input record and return formatted output record"""
    
    line = line.rstrip('\r\n')
    
    # Create output buffer
    output = bytearray(b' ' * output_rec_size)
    
    # Variables for tracking
    dev_imp_decimal = '3'
    dev_ges_decimal = '3'
    dev_ctp_decimal = '3'
    hb_imputation = 'BR'
    top_int_ext = 'E'
    tva = '00'
    dat_ope = ''
    id_lot = ''
    appli_emet = ''
    
    # Process each field
    for i, field in enumerate(field_defs):
        if field['length_input'] <= 0:
            continue
        
        # Extract input field
        start = field['start_pos_input']
        end = start + field['length_input']
        
        if end > len(line):
            return None  # Invalid record length
        
        input_field = line[start:end]
        
        # Process based on field name
        if field['name'] == 'MAI_DEV_IMP':
            dev_imp = input_field.strip()
            if dev_imp == EMPTY_CURRENCY or not dev_imp:
                dev_imp_decimal = '3'
            else:
                dev_imp_decimal = currency_table.get(dev_imp, '3')
            output_field = format_field(input_field, field['format'], field['length_output'])
            output[field['start_pos_output']:field['start_pos_output'] + field['length_output']] = output_field
        
        elif field['name'] == 'MAI_MNT_IMP':
            # Parse and format amount
            success, amt_val, sign, dec_num = parse_amount(input_field)
            if success:
                # Format as amount with sign and decimal
                sign_char = '-' if sign < 0 else '+'
                formatted = sign_char + amt_val.zfill(AMOUNT_FIELD_LENGTH) + str(dec_num)
                corrected = correct_format_amount(formatted, dev_imp_decimal)
                output[field['start_pos_output']:field['start_pos_output'] + field['length_output']] = corrected.encode('latin1')
            else:
                output_field = format_field(input_field, field['format'], field['length_output'])
                output[field['start_pos_output']:field['start_pos_output'] + field['length_output']] = output_field
        
        elif field['name'] == 'MAI_DEV_GES':
            dev_ges = input_field.strip()
            if dev_ges == EMPTY_CURRENCY or not dev_ges:
                dev_ges_decimal = '3'
            else:
                dev_ges_decimal = currency_table.get(dev_ges, '3')
            output_field = format_field(input_field, field['format'], field['length_output'])
            output[field['start_pos_output']:field['start_pos_output'] + field['length_output']] = output_field
        
        elif field['name'] == 'MAI_MNT_GES':
            success, amt_val, sign, dec_num = parse_amount(input_field)
            if success:
                sign_char = '-' if sign < 0 else '+'
                formatted = sign_char + amt_val.zfill(AMOUNT_FIELD_LENGTH) + str(dec_num)
                corrected = correct_format_amount(formatted, dev_ges_decimal)
                output[field['start_pos_output']:field['start_pos_output'] + field['length_output']] = corrected.encode('latin1')
            else:
                output_field = format_field(input_field, field['format'], field['length_output'])
                output[field['start_pos_output']:field['start_pos_output'] + field['length_output']] = output_field
        
        elif field['name'] == 'MAI_DEV_CTP':
            dev_ctp = input_field.strip()
            if dev_ctp == EMPTY_CURRENCY or not dev_ctp:
                dev_ctp_decimal = '3'
            else:
                dev_ctp_decimal = currency_table.get(dev_ctp, '3')
            output_field = format_field(input_field, field['format'], field['length_output'])
            output[field['start_pos_output']:field['start_pos_output'] + field['length_output']] = output_field
        
        elif field['name'] == 'MAI_MNT_NOM':
            success, amt_val, sign, dec_num = parse_amount(input_field)
            if success:
                sign_char = '-' if sign < 0 else '+'
                formatted = sign_char + amt_val.zfill(AMOUNT_FIELD_LENGTH) + str(dec_num)
                corrected = correct_format_amount(formatted, dev_ges_decimal)  # Note: uses dev_ges_decimal
                output[field['start_pos_output']:field['start_pos_output'] + field['length_output']] = corrected.encode('latin1')
            else:
                output_field = format_field(input_field, field['format'], field['length_output'])
                output[field['start_pos_output']:field['start_pos_output'] + field['length_output']] = output_field
        
        elif field['name'] == 'MAI_DAT_OPE':
            dat_ope = input_field[:4]  # First 4 chars (year)
            id_lot = input_field[:DATE_LENGTH]  # Full date (8 chars)
            output_field = format_field(input_field, field['format'], field['length_output'])
            output[field['start_pos_output']:field['start_pos_output'] + field['length_output']] = output_field
        
        elif field['name'] == 'MAI_CPT_IMP':
            # Look up DODGE account
            compte_dodge = input_field.strip()
            if compte_dodge in dodge_table:
                top_bilan, hb_imputation, top_int_ext, tva = dodge_table[compte_dodge]
            else:
                logger.log_reject(f"Dodge Account NOT FOUND: {compte_dodge}")
                hb_imputation = 'BR'
                top_int_ext = 'E'
                tva = '00'
            
            # Update id_lot
            id_lot += hb_imputation + top_int_ext
            
            output_field = format_field(input_field, field['format'], field['length_output'])
            output[field['start_pos_output']:field['start_pos_output'] + field['length_output']] = output_field
            
            # Add extra fields for CD_TYPIMP, CD_TYPEI, CD_TVA_APP
            if field['field_type'] == ADD_CD_TYPIMP_TYPEI_TVA:
                # Next 3 fields are the added fields
                idx_typimp = i + 1
                idx_typei = i + 2
                idx_tva = i + 3
                
                if idx_typimp < len(field_defs):
                    pos = field_defs[idx_typimp]['start_pos_output']
                    length = field_defs[idx_typimp]['length_output']
                    output[pos:pos + length] = hb_imputation.encode('latin1')[:length].ljust(length)
                
                if idx_typei < len(field_defs):
                    pos = field_defs[idx_typei]['start_pos_output']
                    length = field_defs[idx_typei]['length_output']
                    output[pos:pos + length] = top_int_ext.encode('latin1')[:length].ljust(length)
                
                if idx_tva < len(field_defs):
                    pos = field_defs[idx_tva]['start_pos_output']
                    length = field_defs[idx_tva]['length_output']
                    output[pos:pos + length] = tva.encode('latin1')[:length].ljust(length)
        
        elif field['name'] == 'MAI_REF_OPE':
            # Extract NUM_CRE (6 chars at position 11) and APPLI_EMET (3 chars at position 17)
            if len(input_field) >= 20:
                num_cre = input_field[11:11 + NUM_CRE_IN_CD_REFOPER_LENGTH]
                id_lot += num_cre
                appli_emet = input_field[17:20]
            
            output_field = format_field(input_field, field['format'], field['length_output'])
            output[field['start_pos_output']:field['start_pos_output'] + field['length_output']] = output_field
        
        else:
            # Default field processing
            output_field = format_field(input_field, field['format'], field['length_output'])
            output[field['start_pos_output']:field['start_pos_output'] + field['length_output']] = output_field
    
    # Build header
    lot_num, id_ecritu = lot_manager.get_lot_and_id(appli_emet, id_lot)
    
    header = bytearray(b' ' * HEADER_LENGTH)
    header[0:HEADER_CD_CRE_LENGTH] = HEADER_CD_CRE.encode('latin1')[:HEADER_CD_CRE_LENGTH].ljust(HEADER_CD_CRE_LENGTH)
    header[HEADER_CD_CRE_LENGTH:HEADER_CD_CRE_LENGTH + 4] = dat_ope.encode('latin1')[:4].ljust(4)
    header[HEADER_CD_CRE_LENGTH + 4:HEADER_CD_CRE_LENGTH + 7] = appli_emet.encode('latin1')[:3].ljust(3)
    header[HEADER_CD_CRE_LENGTH + 7:HEADER_CD_CRE_LENGTH + 24] = f"{lot_num:017d}".encode('latin1')
    header[HEADER_CD_CRE_LENGTH + HEADER_ID_LOT_LENGTH + HEADER_ID_COMPOST_LENGTH:HEADER_CD_CRE_LENGTH + HEADER_ID_LOT_LENGTH + HEADER_ID_COMPOST_LENGTH + HEADER_ID_ECRITU_LENGTH] = f"{id_ecritu:06d}".encode('latin1')
    
    output[0:HEADER_LENGTH] = header
    
    return bytes(output)


def main() -> int:
    """Main processing function"""
    
    # Parse arguments
    if len(sys.argv) < 3:
        print("---                    U S A G E                    ---")
        print("   Har_Transco_PreMai <Input File> <Accounting Date> [Site]")
        print("   - Input File: Path to input file [Mandatory]")
        print("   - Accounting Date: Date in YYYYMMDD format [Mandatory]")
        print("   - Site: NULL (default) or CASA [Optional]")
        return 1
    
    input_file = sys.argv[1]
    accounting_date = sys.argv[2]
    site = sys.argv[3] if len(sys.argv) >= 4 else "NULL"
    
    # Validate site parameter
    if site not in ["NULL", "CASA"]:
        print("---                    U S A G E                    ---")
        print("   Enter a Valid Site [NULL] or [CASA]")
        return 1
    
    log_process("Start Har_Transco_PreMai Program ...")
    log_process(f"Input File: {input_file}")
    log_process(f"Accounting Date: {accounting_date}")
    log_process(f"Site: {site}")
    
    # Validate accounting date
    if not is_valid_date(accounting_date):
        print("---                    U S A G E                    ---")
        print("   Enter a Valid Accounting Date in YYYYMMDD Format")
        return 1
    
    # Determine configuration directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env_dir = os.getenv("RDJ_DAT") or os.getenv("RDJ_DAT_DIR") or os.getenv("CONFIGURATION_DIRECTORY")
    candidates = [
        env_dir,
        os.path.join(script_dir, "..", "RDJ_DAT"),
        os.path.join(script_dir, "RDJ_DAT"),
        os.path.join(os.getcwd(), "RDJ_DAT"),
        os.path.join(os.getcwd(), "..", "RDJ_DAT"),
        "RDJ_DAT",
    ]
    config_dir = None
    for candidate in candidates:
        if not candidate:
            continue
        candidate = os.path.abspath(candidate)
        if os.path.isfile(os.path.join(candidate, "struct_premai.conf")):
            config_dir = candidate
            break
    
    if not config_dir:
        expected = os.path.abspath("RDJ_DAT")
        log_process("Error loading configuration: struct_premai.conf not found.")
        log_process(f"Searched locations: {', '.join([c for c in candidates if c])}")
        log_process(f"Set RDJ_DAT or RDJ_DAT_DIR to the directory that contains struct_premai.conf.")
        return 1
    
    log_process(f"Configuration Directory: {config_dir}")
    
    # Load configuration
    try:
        field_defs, input_rec_length = load_field_definitions(config_dir, site)
        dodge_table = load_dodge_table(config_dir, site)
        currency_table = load_currency_table(config_dir)
    except Exception as e:
        log_process(f"Error loading configuration: {e}")
        return 1
    
    log_process(f"Loaded {len(field_defs)} field definitions")
    log_process(f"Loaded {len(dodge_table)} DODGE accounts")
    log_process(f"Loaded {len(currency_table)} currencies")
    log_process(f"Input record length: {input_rec_length}")
    
    # Calculate output record size
    output_rec_size = max(f['start_pos_output'] + f['length_output'] for f in field_defs)
    log_process(f"Output record size: {output_rec_size}")
    
    # Prepare output file
    output_file = input_file + OUTPUT_FILE_EXTENSION
    
    # Initialize LOT manager
    lot_manager = LOTManager()
    
    # Open reject file
    logger.open_reject_file(input_file)
    
    try:
        log_process(f"Processing {input_file} -> {output_file}")
        
        record_count = 0
        empty_count = 0
        start_time = datetime.now()
        
        with open(input_file, 'r', encoding='utf-8', buffering=BUFFER_SIZE) as fin, \
             open(output_file, 'wb', buffering=BUFFER_SIZE) as fout:
            
            for line in fin:
                if len(line) <= 2:
                    empty_count += 1
                    continue
                
                # Check record length
                if len(line.rstrip('\r\n')) != input_rec_length - 1:
                    logger.log_reject(f"Invalid record length: expected {input_rec_length - 1}, got {len(line.rstrip())}")
                    continue
                
                output_rec = process_record(
                    line, field_defs, dodge_table, currency_table, 
                    lot_manager, output_rec_size
                )
                
                if output_rec:
                    fout.write(output_rec + b'\n')
                    record_count += 1
                else:
                    logger.log_reject(f"Failed to process record: {line.rstrip()}")
                
                if record_count % 100000 == 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = record_count / elapsed if elapsed > 0 else 0
                    log_process(f"Processed {record_count:,} records... ({rate:,.0f} rec/sec)")
        
        elapsed = (datetime.now() - start_time).total_seconds()
        rate = record_count / elapsed if elapsed > 0 else 0
        
        log_process(f"Total Records Read : {record_count + empty_count:,}")
        log_process(f"Total Records Processed : {record_count:,}")
        log_process(f"Total Rejects : {logger.reject_count:,}")
        log_process(f"Processing Time : {elapsed:.2f} seconds ({rate:,.0f} rec/sec)")
        log_process("End Har_Transco_PreMai Program ...")
        
    finally:
        logger.close()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
