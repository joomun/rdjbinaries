/* 
 *=======================================================================================
 * NAME        : Har_Transco_PreMai.c
 * DESCRIPTION : Enrichment and Formatting of ARC_PRE_MAI Data File for RDJ Handling
 * AUTHOR      : Z. BERRAH
 * CREATION    : 2011/11/25
 * UPDATE      : 2013/09/16
 *=======================================================================================
 *                                  U S A G E
 *
 *         Har_Transco_PreMai <Parameter 1> <Parameter 2>
 *       - Parameter 1 : Input File                         [Mandatory]
 *       - Parameter 2 : Accounting Date in YYYYMMDD Format [Mandatory]
 *		 - Parameter 3 : Site [if "NULL" : Default Site is CACIB+SST+LCL (PPCO dodge CACIB) / If "CASA" : Site is CASA (PPCO dodge CASA)]
 *
 *=======================================================================================
 *
 *---------------------------------------------------------------------------------------
 * DATE       | NAME             | DESCRIPTION
 *---------------------------------------------------------------------------------------
 * 2011/11/25 | Z. BERRAH        | Optimized version including Hash Coding
 *=======================================================================================
 */

/* Includes */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <math.h>
#include <sys/types.h>
#include <sys/timeb.h>
#include <ctype.h>

/* Additional Functions */
#define SIGN(x)  ((x) < 0 ? ('-') : ('+'))
#define ROUND(x) ((x - floor(x)) < 0.5 ? (floor(x)) : (ceil(x)))

/* Common Constants */
#define DATE_LENGTH						8
#define TRUE							1
#define FALSE							0
#define EXIT_OK							0
#define EXIT_ERR						1
#define NB_PARAM						3 // 2 + 1 : Input File and Accounting Date
#define AMOUNT_FIELD_LENGTH				18
#define AMOUNT_DECIMAL_NR				"3"
#define SIGN_FIELD_LENGTH				1
#define DECIMAL_NR_FIELD_LENGTH			1
#define DECIMAL_SYMBOL					'.'
#define FIELD_NAME_LENGTH				50
#define FIELD_FORMAT_LENGTH				20
#define MAX_FIELD_LENGTH				500				
#define MAX_FIELD_NUMBER				400
#define SEPARATOR 						";"

/* Hash Key Handling */
#define HASH_DODGE_ARRAY_SIZE  			2000
#define HASH_CURRENCY_ARRAY_SIZE		50
#define HASH_LOT_ARRAY_SIZE				2000
#define HASH_KEY_FOUND			   		1
#define HASH_KEY_NOT_FOUND				0
#define MAX_HASH_KEY_LENGTH				18	// Hash Key is a 'long long' variable. Its Maximal Value is : 9 223 372 036 854 775 807
											// The string "zzzzzzzzzzzzzzzzzz" gives the highest value of Hash Key : 9 999 999 999 999 999 990

/* File Handling */
#define MAX_INPUT_REC_LENGTH			1000
#define MAX_INPUT_FORMAT_REC_LENGTH		100
#define HEADER_LENGTH					169
#define HEADER_CD_CRE					"CPTA_MARCHE"
#define HEADER_CD_CRE_LENGTH			20
#define HEADER_ID_LOT_LENGTH			24
#define HEADER_ID_COMPOST_LENGTH		25
#define HEADER_ID_ECRITU_LENGTH			6
#define MAX_ID_ECRITU					999999
#define HEADER_CH_FILLER1_LENGTH		94
#define OUTPUT_FILE_EXTENSION			".out"
#define CONFIGURATION_DIRECTORY			"RDJ_DAT"
#define INPUT_FILE_FORMAT_NAME			"struct_premai.conf"
#define REF_RCA_CPT_CAS_FILE_NAME		"REF_RCA_CPT_CAS.dat"
#define REF_RCA_CPT_FILE_NAME			"REF_RCA_CPT.dat"
#define REF_RCA_CPT_FORMAT_FILE_NAME	"REF_RCA_CPT.conf"
#define REF_RCA_CPT_FIELD_NAME_LENGTH	50
#define REF_CURRENCY_FORMAT_FILE_NAME	"REF_CURRENCY.conf"
#define REF_CURRENCY_FILE_NAME			"REF_CURRENCY.dat"
#define MAX_DIRECTORY_LENGTH			500
#define MAX_FULL_FILE_NAME_LENGTH		500

/* DODGE Informations */
#define REF_RCA_CPT_RECORD_LENGTH		3000
#define REF_RCA_CPT_SEPARATOR			";"
#define MAX_SEPARATOR_IN_REF_RCA_CPT	54	// Number of separators in REF_RCA_CPT.dat File
#define MAX_FIELD_NUMBER_REF_RCA_CPT	55	// Number of records in REF_RCA_CPT.conf File
#define ADD_CD_TYPIMP_TYPEI_TVA			10
#define UPDATE_MAI_MNT_IMP				110
#define UPDATE_MAI_MNT_GES				120
#define UPDATE_MAI_MNT_NOM				130
#define CD_TYPIMP_TYPEI_TVA_INPUT_POSIT	-10
#define CD_TYPIMP_FIELD_LENGTH			2	// Bilan, Hors Bilan : "BR", "HB"
#define CD_TYPEI_FIELD_LENGTH			1	// Externe, Interne  : "E",  "I", "M"
#define CD_TVA_APP_FIELD_LENGTH			2	// TVA : "00", "01", "02", "03", "10", "11", "12", "13"
#define	NUM_CRE_IN_CD_REFOPER_LENGTH	6

/* Currency Informations */
#define MAX_CURRENCY_FORMAT_REC_LENGTH	100
#define REF_CURRENCY_FIELD_NAME_LENGTH	50
#define REF_CURRENCY_RECORD_LENGTH		50
#define REF_CURRENCY_SEPARATOR			";"
#define MAX_SEPARATOR_IN_REF_CURRENCY	1	// Number of separators in REF_CURRENCY.dat File
#define MAX_FIELD_NUMBER_CURRENCY		2	// Number of records in REF_CURRENCY.conf File
#define CURRENCY_CD_LENGTH				3
#define EMPTY_CURRENCY					"   "

char RefRcaCpt_Record[REF_RCA_CPT_RECORD_LENGTH];
char ENTITY[10];
char RefCurrency_Record[REF_CURRENCY_RECORD_LENGTH];
char *strConfigurationDirectory			= NULL;
char strRCA_CPT_FILE_NAME[18+1];
long iLastLOT_NUM_USED					= 0;
long iInputRecordLength					= 1;	// Record Length of Input File given by struct_premai.conf
	
typedef enum 
{
	skip=0,			// Unchanged Original Value
	charED=1,		// String Right space filled
	charEG=2,		// String Left  space filled 
	entierEG=3,		// Unsigned Numeric  with Left spaces
	entierZG=4,		// Unsigned Numeric  with Left zeroes
	entierSG=5,		// Signed Numeric with Left spaces - The sign is at the Left side
	entierSZG=6,	// Signed Numeric with Left zeroes - The sign is at the Left side
	entierSD=7,		// Signed Numeric with Left spaces - The sign is at the Right side
	entierSZD=8,	// Signed Numeric with Left zeroes - The sign is at the Right side
	amount3DEC=9,	// Amount with 3 decimals
	unknown=-1		// Unknown Format
}	enumFieldFormat;

/* Useful Data extracted from REF_RCA_CPT.dat File */ 
struct
{
	char strCOMPTE_DODGE[15 + 1];
	char strTOP_BILAN[1 + 1];
	char strTOP_INT_EXT[CD_TYPEI_FIELD_LENGTH + 1];
	char strTVA[CD_TVA_APP_FIELD_LENGTH + 1];
}	RefRcaCptFile_Struct;

/* Data of REF_CURRENCY.dat File */ 
struct
{
	char strCURRENCY_CD[CURRENCY_CD_LENGTH + 1];
	char strDECIMAL_POS[1 + 1];
}	RefCurrencyFile_Struct;

/* COMPTE_DODGEHashArray Table */
typedef struct stCOMPTE_DODGEHashElt
{
	long long	COMPTE_DODGEHashKey;
	char 		strCOMPTE_DODGE[15 + 1];
	char 		strTOP_BILAN[1 + 1];
	char		strHB_IMPUTATION[CD_TYPIMP_FIELD_LENGTH + 1];
	char 		strTOP_INT_EXT[CD_TYPEI_FIELD_LENGTH + 1];
	char		strTVA[CD_TVA_APP_FIELD_LENGTH + 1];
}	COMPTE_DODGEHashElt;

struct
{
	COMPTE_DODGEHashElt		stElt[HASH_DODGE_ARRAY_SIZE];
}	COMPTE_DODGEHashArray[HASH_DODGE_ARRAY_SIZE];

/* CURRENCYHashArray Table */
typedef struct stCURRENCYHashElt
{
	long long	CURRENCYHashKey;
	char 		strCURRENCY_CD[CURRENCY_CD_LENGTH + 1];
	char		strDECIMAL_POS[1 + 1];
}	CURRENCYHashElt;

struct
{
	CURRENCYHashElt		stElt[HASH_CURRENCY_ARRAY_SIZE];
}	CURRENCYHashArray[HASH_CURRENCY_ARRAY_SIZE];

/* LOTHashArray Table */
typedef struct stLOTHashElt
{
	long long	LOTHashKey;
	char		strAPPLI_EMET_ID_LOT[20 + 1];
	char		strAPPLI_EMET[3 + 1];
	char		strID_LOT[17 + 1];
	long		iLOT_NUM;
	long		iID_ECRITU;
}	LOTHashElt;

struct
{
	LOTHashElt	stElt[HASH_LOT_ARRAY_SIZE];
}	LOTHashArray[HASH_LOT_ARRAY_SIZE];

/* Table of the different Fields of the Output File Record */
struct
{
	char			strFieldName[FIELD_NAME_LENGTH];
	char			strFieldFormat[FIELD_FORMAT_LENGTH];
	enumFieldFormat	iFieldFormat;
	int				iFieldType;
	int				iFieldLengthInput;
	int				iFieldLengthOutput;
	int				iFieldStartPosInput;
	int				iFieldStartPosOutput;
}	tabFieldOfRecord[MAX_FIELD_NUMBER];

/* Table of the different Fields of the REF_RCA_CPT.dat or REF_RCA_CPT_CAS.dat File */
struct
{
	char			strFieldName[FIELD_NAME_LENGTH];
	char			strFieldFormat[FIELD_FORMAT_LENGTH];
	enumFieldFormat	iFieldFormat;
	int				iFieldLength;
	int				iFieldStartSepPosition;
}	tabFieldOfRefRcaCptRecord[MAX_FIELD_NUMBER_REF_RCA_CPT + 1];

/* Table of the different Fields of the REF_CURRENCY.dat File */
struct
{
	char			strFieldName[FIELD_NAME_LENGTH];
	char			strFieldFormat[FIELD_FORMAT_LENGTH];
	enumFieldFormat	iFieldFormat;
	int				iFieldLength;
	int				iFieldStartSepPosition;
}	tabFieldOfRefCurrencyRecord[MAX_FIELD_NUMBER_CURRENCY + 1];

/* Record of struct_premai.conf File */
struct
{
	char InputFormat_Record[MAX_INPUT_FORMAT_REC_LENGTH];
}	Input_File_Record_Format_Struct;

/* Record of REF_CURRENCY.conf File */
struct
{
	char CurrencyFormat_Record[MAX_CURRENCY_FORMAT_REC_LENGTH];
}	Currency_File_Record_Format_Struct;

/* Input File */
struct
{
	char Input_Record[MAX_INPUT_REC_LENGTH];
}	Input_Record_Struct;

/* Output File */
struct
{
	char Output_Header[HEADER_LENGTH + 1];
	char Output_Record[MAX_INPUT_REC_LENGTH + 3 * (SIGN_FIELD_LENGTH + DECIMAL_NR_FIELD_LENGTH) + CD_TYPIMP_FIELD_LENGTH + CD_TYPEI_FIELD_LENGTH + CD_TVA_APP_FIELD_LENGTH + 1];
}	Output_Record_Struct;

/* 
 * =============================================================================
 *               Which Output Format for the Field ?
 * =============================================================================
 */
int whichOutputFormat (const char *i_strFieldFormat)
{
	if (strcmp(i_strFieldFormat, "skip") == 0)
	{
		return skip;
	}
	if (strcmp(i_strFieldFormat, "charED") == 0)
	{
		return charED;
	}
	if (strcmp(i_strFieldFormat, "charEG") == 0)
	{
		return charEG;
	}
	if (strcmp(i_strFieldFormat, "entierEG") == 0)
	{
		return entierEG;
	}
	if (strcmp(i_strFieldFormat, "entierZG") == 0)
	{
		return entierZG;
	}
	if (strcmp(i_strFieldFormat, "entierSG") == 0)
	{
		return entierSG;
	}
	if (strcmp(i_strFieldFormat, "entierSZG") == 0)
	{
		return entierSZG;
	}
	if (strcmp(i_strFieldFormat, "entierSD") == 0)
	{
		return entierSD;
	}
	if (strcmp(i_strFieldFormat, "entierSZD") == 0)
	{
		return entierSZD;
	}
	if (strcmp(i_strFieldFormat, "amount3DEC") == 0)
	{
		return amount3DEC;
	}
	return unknown;
}

/* 
 * =============================================================================
 *              Is the Year of the Accounting Date a Leap Year ?
 * =============================================================================
 */
int isLeapYear (int i_iYear)
{
	if ((i_iYear%4) != 0)			// i_iYear is not divisible by 4
	{
		return FALSE;
	}
	else
	{
		if ((i_iYear%400) == 0)		// Leap Year : i_iYear is divisible by 400
		{
			return TRUE;
		}
		else
		{
			if ((i_iYear%100) == 0)	// i_iYear is divisible by 100
			{
				return FALSE;
			}
			else
			{
				return TRUE;		// Leap Year : i_iYear is divisible by 4 but not by 100
			}
		}
	}
}

/*
 * =============================================================================
 *                        Is the Input Date Valid ?
 * =============================================================================
*/
int isValidInputDate (const char *i_strDate)
{
	char  l_strYear[4+1];
	char  l_strMonth[2+1];
	char  l_strDay[2+1];
	int   l_iIdx   = 0;
	int   l_iYear  = 0;
	int   l_iMonth = 0;
	int   l_iDay   = 0;
	int   l_itabNbDaysInMonth[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
	
	/* Input Date must be Numeric */
	for(l_iIdx=0; l_iIdx < DATE_LENGTH; l_iIdx++)
	{
		if (! isdigit(i_strDate[l_iIdx]))
		{
			printf("The Input Date %s is Invalid !!!  \n", i_strDate);
			return FALSE;
		}
	}
	/* Numeric Imput Date must be Valid */
	memcpy(l_strYear,  i_strDate, 4);
	l_strYear[4]  = '\0';
	memcpy(l_strMonth, i_strDate + 4, 2);
	l_strMonth[2] = '\0';
	memcpy(l_strDay,   i_strDate + 6, 2);
	l_strDay[2]   = '\0';
	l_iYear  = atoi(l_strYear);
	l_iMonth = atoi(l_strMonth);
	l_iDay   = atoi(l_strDay);
	
    if (isLeapYear(l_iYear))
        l_itabNbDaysInMonth[2] = 29;	// 29 days in February when Leap Year
    
    if ( l_iMonth < 1 || l_iMonth > 12 )
	{
        return FALSE;
	}
    else
	{
		if ( l_iDay < 1 || l_iDay > l_itabNbDaysInMonth[l_iMonth] )
		{
			return FALSE;
		}
	}
    return TRUE;
}

/* 
 * =============================================================================
 *             Suppress zeroes at the Left side of a string
 * =============================================================================
 */
void LZTrim(char *i_strField)
{
	int l_iIdxR = 0;
	int l_iIdxL = 0;
	int l_iFieldLength = (strlen(i_strField) - 1);

	if (strlen(i_strField))
	{
		// The i_strField is not Empty
		// Find the First Character which is not a zero from the Left side of i_strField
		while (i_strField[l_iIdxR] == '0')
			l_iIdxR++;

		if (l_iIdxR <= l_iFieldLength)
		{
			// Zeroes found at the Left side of i_strField
			// Move all the characters to the Left
			for (l_iIdxL = 0; l_iIdxL <= (l_iFieldLength - l_iIdxR); l_iIdxL++)
			{
				i_strField[l_iIdxL] = i_strField[(l_iIdxR + l_iIdxL)];
			}
			i_strField[l_iIdxL] = '\0';
		}
		else
		{
			// The i_strField contains only zeroes. Suppress them all.
			i_strField[0] = '\0';
		}
	}
}

/* 
 * =============================================================================
 *             Suppress spaces at the Left side of a string
 * =============================================================================
 */
void LTrim(char *i_strField)
{
	int l_iIdxR = 0;
	int l_iIdxL = 0;
	int l_iFieldLength = (strlen(i_strField) - 1);

	if (strlen(i_strField))
	{
		// The i_strField is not Empty
		// Find the First Character which is not a space from the Left side of i_strField
		while (i_strField[l_iIdxR] == ' ')
			l_iIdxR++;

		if (l_iIdxR <= l_iFieldLength)
		{
			// Spaces found at the Left side of i_strField
			// Move all the characters to the Left
			for (l_iIdxL = 0; l_iIdxL <= (l_iFieldLength - l_iIdxR); l_iIdxL++)
			{
				i_strField[l_iIdxL] = i_strField[(l_iIdxR + l_iIdxL)];
			}
			i_strField[l_iIdxL] = '\0';
		}
		else
		{
			// The i_strField contains only spaces. Suppress them all.
			i_strField[0] = '\0';
		}
	}
}

/* 
 * =============================================================================
 *             Suppress spaces at the Right side of a string
 * =============================================================================
 */
void RTrim(char *i_strField)
{
	int l_iIdx = 0;
	int l_iFieldLength = (strlen(i_strField) - 1);

	if (strlen(i_strField))
	{
		// The i_strField is not Empty
		// Find the First Character which is not a space from the Right side of i_strField
		l_iIdx = l_iFieldLength;
		while (i_strField[l_iIdx] == ' ')
			l_iIdx--;

		// If any spaces at the Right side of i_strField, suppress them by shorthening i_strField using '\0'
		if (l_iIdx >= -1)
			i_strField[l_iIdx + 1] = '\0';
	}
}

/* 
 * =============================================================================
 *         Suppress spaces at the Left and the Right sides of a string
 * =============================================================================
 */
void Trim(char *i_strField)
{
	// Suppress spaces at the Left side of i_strField
	LTrim(i_strField);
	// Suppress spaces at the Right side of i_strField
	RTrim(i_strField);
}

/* 
 * =============================================================================
 *          Is the Field Numeric ? Which is its Sign ?
 * =============================================================================
 */
int isNumeric(const char *i_strField, char *o_strField, int *o_iSign)
{
	char l_strSign		= ' ';
	int  l_iIdx         = 0;
	int  l_iSignPosit   = 0;
	int  l_iFieldLength = (strlen(i_strField) - 1);
	
	// Initialize Output Field
	strcpy(o_strField, i_strField);
	
	// printf(" - [isNumeric] - i_strField = %s, o_strField = %s, l_iFieldLength = %d, o_iSign = %d.\n", i_strField, o_strField, l_iFieldLength, *o_iSign);
	if (strlen(o_strField))	
	{
		// The Field is not Empty
		for(l_iIdx=0; l_iIdx < strlen(o_strField); l_iIdx++)
		{
			if ((o_strField[l_iIdx] == '+') || (o_strField[l_iIdx] == '-'))
			{
				l_strSign    = o_strField[l_iIdx];
				l_iSignPosit = l_iIdx;
				// We leave because we expect to find only one Sign in the Field
				l_iIdx       = strlen(o_strField);
			}
		}
		if ((l_strSign == '+') || (l_strSign == '-'))
		{
			// Sign found at Position l_iSignPosit
			// Suppress the Sign and Move all the other characters to the Left till Sign Position
			for (l_iIdx = l_iSignPosit; l_iIdx < l_iFieldLength; l_iIdx++)
			{
				o_strField[l_iIdx] = o_strField[l_iIdx + 1];
			}
			o_strField[l_iIdx] = '\0';
		}
		else
		{
			// When the Sign is not found, we suppose the Sign equal to '+'
			l_strSign = '+';
		}
		if (l_strSign == '+')
		{
			*o_iSign = 1;
		}
		else
		{
			*o_iSign = -1;
		}
		
		// Suppress spaces at the Left and the Right sides of o_strField
		Trim(o_strField);
		// Check if o_strField is Numeric
		for(l_iIdx=0; l_iIdx < strlen(o_strField); l_iIdx++)
		{
			if (! isdigit(o_strField[l_iIdx]))
			{
				strcpy(o_strField, i_strField);
				*o_iSign = 0;
				// printf(" - [isNumeric] - i_strField = %s, o_strField = %s, o_iSign = %d.\n", i_strField, o_strField, *o_iSign);
				return FALSE;
			}
		}
		// printf(" - [isNumeric] - i_strField = %s, o_strField = %s, o_iSign = %d.\n", i_strField, o_strField, *o_iSign);
		return TRUE;
	}
	else
	{
		// printf(" - [isNumeric] - i_strField = %s, o_strField = %s, o_iSign = %d.\n", i_strField, o_strField, *o_iSign);
		return FALSE;
	}
}

/*
 * =============================================================================
 *                        Is the Input Amount Valid ?
 * =============================================================================
*/
int isValidInputAmount (char *i_strAmount, char *o_strAmount, int *o_iSign, int *o_iDecimalNumber)
{
	char l_strAmount[MAX_FIELD_LENGTH];
	int  l_iIdx   				= 0;
	int  l_iSign  				= 0;
	int  l_iDecimalSymbolPos	= -1;
	int  l_iDecimalNumber		= 0;
	int  l_iAmountLength = (strlen(i_strAmount) - 1);
	int  l_iLastSpacePos = strlen(i_strAmount);
	
	// Initialize Output Amount
	strcpy(l_strAmount, i_strAmount);

	// Evaluate the position of the Decimal Symbol in l_strAmount
	for (l_iIdx = l_iAmountLength; l_iIdx > -1; l_iIdx--)
	{
		if (l_strAmount[l_iIdx] == ' ')
		{
			l_iLastSpacePos = l_iIdx;
		}
		else
		{
			if (l_strAmount[l_iIdx] == DECIMAL_SYMBOL)
			{
				l_iDecimalSymbolPos = l_iIdx;
				l_iIdx = -1;
			}
		}
	}
	// Decimal Symbol found. Evaluate the Number of decimals
	if (l_iDecimalSymbolPos != -1)
	{
		l_iDecimalNumber = l_iLastSpacePos - l_iDecimalSymbolPos - 1;
		// Suppress the Decimal Symbol
		// Move to the Left all the characters at the right side of the Decimal Symbol
		for (l_iIdx = l_iDecimalSymbolPos; l_iIdx < l_iLastSpacePos - 1; l_iIdx++)
		{
			l_strAmount[l_iIdx] = l_strAmount[(l_iIdx + 1)];
		}
		l_strAmount[l_iLastSpacePos - 1] = '\0';
	}
	// Is The Amount Numeric ? Which is its Sign ?
	if (isNumeric(l_strAmount, o_strAmount, &l_iSign))
	{
		// Valid Amount. Suppress zeroes at the Left side of o_strAmount
		LZTrim(o_strAmount);
		*o_iSign = l_iSign;
		*o_iDecimalNumber = l_iDecimalNumber;
		return TRUE;
	}
	else
	{
		// Invalid Amount
		strcpy(o_strAmount, i_strAmount);
		*o_iSign          = 0;
		*o_iDecimalNumber = 0;
		return FALSE;
	}
}

/*
 * =============================================================================
 *                        Correct the Format of an Amount
 * =============================================================================
*/
void CorrectFormatAmount(const char *i_strAmount, const char *i_strCurDecimalNr, char *o_strAmount)
{
	char	l_strCurDecimalNr[1 + 1];
	char	l_strAmtDecimalNr[1 + 1];
	char 	l_strAmount[AMOUNT_FIELD_LENGTH + 1];
	int  	l_iDiffCurDecimalNr	= 0;
	int  	l_iAmountLength 	= (strlen(i_strAmount) - 1);
	double 	l_lAmount			= 0;
	double 	Exp 				= 0;
	double 	Ratio 				= 0;
	double 	Numerator 			= 0;
	double 	Denominator 		= 0;

	// Initialize Output Amount and Number of Decimals of the Currencies
	strcpy(o_strAmount, i_strAmount);
	strcpy(l_strCurDecimalNr, i_strCurDecimalNr);
	l_strAmtDecimalNr[0] = i_strAmount[l_iAmountLength];
	l_strAmtDecimalNr[1] = '\0';
	// printf(" - [CorrectFormatAmount] - String  Values : i_strAmount = %s, l_strAmtDecimalNr = %s, l_strCurDecimalNr = %s.\n", i_strAmount, l_strAmtDecimalNr, l_strCurDecimalNr);
	// printf(" - [CorrectFormatAmount] - Numeric Values : l_strAmtDecimalNr = %d, l_strCurDecimalNr = %d.\n", atoi(l_strAmtDecimalNr), atoi(l_strCurDecimalNr));
	
	// The Output Amount must have 3 Decimals
	// The Amount must be updated when the Number of Decimals of the Currency
	// is different from the Number of Decimals of the Input Amount
	if (atoi(l_strCurDecimalNr) == atoi(l_strAmtDecimalNr))
	{
		memset(o_strAmount, '0', 20);
		memcpy(o_strAmount, i_strAmount, 1);	// Sign 
		memcpy(o_strAmount + 1, i_strAmount + 1 + 3 - atoi(l_strCurDecimalNr), AMOUNT_FIELD_LENGTH - 3 + atoi(l_strCurDecimalNr));	// Amount
		memcpy(o_strAmount + 1 + AMOUNT_FIELD_LENGTH, AMOUNT_DECIMAL_NR, 1);	// Number of Decimals
		o_strAmount[20] = '\0';
		// printf (" - [CorrectFormatAmount] - o_strAmount = %s\n", o_strAmount);
	}
	else
	{
		if (atoi(l_strCurDecimalNr) > atoi(l_strAmtDecimalNr))
		{
			l_iDiffCurDecimalNr = atoi(l_strCurDecimalNr) - atoi(l_strAmtDecimalNr);
			// printf(" - [CorrectFormatAmount] - l_iDiffCurDecimalNr = %d\n", l_iDiffCurDecimalNr);
			memset(o_strAmount, '0', 20);
			memcpy(o_strAmount, i_strAmount, 1);	// Sign 
			memcpy(o_strAmount + 1, i_strAmount + 1 + l_iDiffCurDecimalNr, AMOUNT_FIELD_LENGTH - l_iDiffCurDecimalNr);	// Amount
			memcpy(o_strAmount + 1 + AMOUNT_FIELD_LENGTH, AMOUNT_DECIMAL_NR, 1);	// Number of Decimals
			o_strAmount[20] = '\0';
			// printf (" - [CorrectFormatAmount] - o_strAmount = %s\n", o_strAmount);
		}
		else
		{
			l_iDiffCurDecimalNr = atoi(l_strAmtDecimalNr) - atoi(l_strCurDecimalNr);
			// printf(" - [CorrectFormatAmount] - l_iDiffCurDecimalNr = %d\n", l_iDiffCurDecimalNr);
			
			Exp = (double) (3 - atoi(l_strCurDecimalNr));	// 3 because Number of Decimals must be 3
			memcpy(l_strAmount, i_strAmount + 1, AMOUNT_FIELD_LENGTH);
			l_strAmount[AMOUNT_FIELD_LENGTH] = '\0';
			
			Numerator = atof(l_strAmount);
			Denominator = pow((double)10, (double)l_iDiffCurDecimalNr);
			// printf(" - [CorrectFormatAmount] - l_strAmount = %s, Numerator = %18.0f, Denominator = %18.0f\n", l_strAmount, Numerator, Denominator);
			// Ratio = Numerator / Denominator;
			// printf(" - [CorrectFormatAmount] - Ratio = %18.3f\n", Ratio);
			Ratio = ROUND((double)(Numerator / Denominator));
			// printf(" - [CorrectFormatAmount] - Rounded Ratio = %18.0f\n", Ratio);
			memset(l_strAmount, '\0', AMOUNT_FIELD_LENGTH);
			sprintf(l_strAmount, "%018.0f", Ratio);
			// printf (" - [CorrectFormatAmount] - l_strAmount = %s.\n", l_strAmount);
			memset(o_strAmount, '0', 20);
			memcpy(o_strAmount, i_strAmount, 1);	// Sign
			memcpy(o_strAmount + 1, l_strAmount + (int)Exp, AMOUNT_FIELD_LENGTH - (int)Exp);	// Amount Corresponds to : pow(10,Exp) * Ratio
			memcpy(o_strAmount + 1 + AMOUNT_FIELD_LENGTH, AMOUNT_DECIMAL_NR, 1);	// Number of Decimals must be 3
			o_strAmount[20] = '\0';
			// printf (" - [CorrectFormatAmount] - o_strAmount = %s\n", o_strAmount);
		}
	}
}

/*
 * =============================================================================
 *              Initialize COMPTE_DODGE Hash Key Table
 * =============================================================================
*/
void InitializeCOMPTE_DODGEHashKeyTable	()
{
	long	l_lIdX;
	long	l_lIdY;
	
	for (l_lIdX = 0; l_lIdX < HASH_DODGE_ARRAY_SIZE; l_lIdX++)
	{
		for (l_lIdY = 0; l_lIdY < HASH_DODGE_ARRAY_SIZE; l_lIdY++)
		{
			COMPTE_DODGEHashArray[l_lIdX].stElt[l_lIdY].COMPTE_DODGEHashKey = -1;
			memset(COMPTE_DODGEHashArray[l_lIdX].stElt[l_lIdY].strCOMPTE_DODGE, '\0', sizeof(COMPTE_DODGEHashArray[l_lIdX].stElt[l_lIdY].strCOMPTE_DODGE));
			memset(COMPTE_DODGEHashArray[l_lIdX].stElt[l_lIdY].strTOP_BILAN,    '\0', sizeof(COMPTE_DODGEHashArray[l_lIdX].stElt[l_lIdY].strTOP_BILAN));
			memset(COMPTE_DODGEHashArray[l_lIdX].stElt[l_lIdY].strHB_IMPUTATION,'\0', sizeof(COMPTE_DODGEHashArray[l_lIdX].stElt[l_lIdY].strHB_IMPUTATION));
			memset(COMPTE_DODGEHashArray[l_lIdX].stElt[l_lIdY].strTOP_INT_EXT,  '\0', sizeof(COMPTE_DODGEHashArray[l_lIdX].stElt[l_lIdY].strTOP_INT_EXT));
			memset(COMPTE_DODGEHashArray[l_lIdX].stElt[l_lIdY].strTVA,          '\0', sizeof(COMPTE_DODGEHashArray[l_lIdX].stElt[l_lIdY].strTVA));
		}
	}
}

/*
 * =============================================================================
 *              Initialize CURRENCY Hash Key Table
 * =============================================================================
*/
void InitializeCURRENCYHashKeyTable	()
{
	long	l_lIdX;
	long	l_lIdY;
	
	for (l_lIdX = 0; l_lIdX < HASH_CURRENCY_ARRAY_SIZE; l_lIdX++)
	{
		for (l_lIdY = 0; l_lIdY < HASH_CURRENCY_ARRAY_SIZE; l_lIdY++)
		{
			CURRENCYHashArray[l_lIdX].stElt[l_lIdY].CURRENCYHashKey = -1;
			memset(CURRENCYHashArray[l_lIdX].stElt[l_lIdY].strCURRENCY_CD, '\0', sizeof(CURRENCYHashArray[l_lIdX].stElt[l_lIdY].strCURRENCY_CD));
			memset(CURRENCYHashArray[l_lIdX].stElt[l_lIdY].strDECIMAL_POS, '\0', sizeof(CURRENCYHashArray[l_lIdX].stElt[l_lIdY].strDECIMAL_POS));
		}
	}
}	

/*
 * =============================================================================
 *              Initialize LOT Hash Key Table
 * =============================================================================
*/
void InitializeLOTHashKeyTable ()
{
	long	l_lIdX;
	long	l_lIdY;
	
	for (l_lIdX = 0; l_lIdX < HASH_LOT_ARRAY_SIZE; l_lIdX++)
	{
		for (l_lIdY = 0; l_lIdY < HASH_LOT_ARRAY_SIZE; l_lIdY++)
		{
			LOTHashArray[l_lIdX].stElt[l_lIdY].LOTHashKey = -1;
			memset(LOTHashArray[l_lIdX].stElt[l_lIdY].strAPPLI_EMET_ID_LOT, '\0', sizeof(LOTHashArray[l_lIdX].stElt[l_lIdY].strAPPLI_EMET_ID_LOT));
			memset(LOTHashArray[l_lIdX].stElt[l_lIdY].strAPPLI_EMET,        '\0', sizeof(LOTHashArray[l_lIdX].stElt[l_lIdY].strAPPLI_EMET));
			memset(LOTHashArray[l_lIdX].stElt[l_lIdY].strID_LOT,            '\0', sizeof(LOTHashArray[l_lIdX].stElt[l_lIdY].strID_LOT));
			LOTHashArray[l_lIdX].stElt[l_lIdY].iLOT_NUM   = -1;
			LOTHashArray[l_lIdX].stElt[l_lIdY].iID_ECRITU = -1;
		}
	}
}	

/*
 * =============================================================================
 *   Build Hash Key used to access COMPTE_DODGE, CURRENCY and LOT Tables
 * =============================================================================
*/
long BuildHashKey (const char i_strKey[], const long i_HashArraySize, long long *o_llHashKey)
{
	long		l_lIdY				= 0;
	long		l_lLength			= strlen (i_strKey);
	long		l_lPositYHashKey	= 0;
	long long	l_llCoef			= 1;
	long long	l_llHashKey			= 0;

	// printf(" - [BuildHashKey] - i_strKey = %s, l_lLength = %ld\n", i_strKey, l_lLength);
	for (l_lIdY = l_lLength - 1; l_lIdY >= 0; l_lIdY--)
	{
		if ((l_lLength - l_lIdY) <= MAX_HASH_KEY_LENGTH)
		{
			if (i_strKey[l_lIdY] < ' ')									// ASCII Code of ' ' : 32
			{
				l_llHashKey	+= i_strKey[l_lIdY] * l_llCoef;
			}
			else
			{
				l_llHashKey	+= (i_strKey[l_lIdY] - ' ') * l_llCoef;		// Example : i_strKey[l_lIdY] : 'A' => 65 - 32 = 33
			}
			l_llCoef *= 10;
		}
		else
		{
			// Unexpected Key Length
			printf(" - [BuildHashKey] - Unexpected Key Length : %ld > %ld. Truncated Hash Key %lld built for Key %s\n", l_lLength, MAX_HASH_KEY_LENGTH, l_llHashKey, i_strKey);
			l_lIdY = -1;
		}
	}

	l_lPositYHashKey = l_llHashKey	% i_HashArraySize;
	*o_llHashKey     = l_llHashKey;
	// printf(" - [BuildHashKey] - l_llHashKey = %lld, l_lPositYHashKey = %ld\n", l_llHashKey, l_lPositYHashKey);
	return l_lPositYHashKey;
}

/*
 * =============================================================================
 *                   Add COMPTE_DODGE HaskKey in COMPTE_DODGEHashArray Table
 * =============================================================================
*/
void AddElementInCOMPTE_DODGEHashKeyTable ()
{
	long		l_lPositYHashKey	= 0;
	long		l_lIdX				= 0;
	long long   l_llHashKey			= 0;

	l_lPositYHashKey = BuildHashKey (RefRcaCptFile_Struct.strCOMPTE_DODGE, HASH_DODGE_ARRAY_SIZE, &l_llHashKey);

	for (l_lIdX = 0; l_lIdX < HASH_DODGE_ARRAY_SIZE; l_lIdX++)
	{
		if (COMPTE_DODGEHashArray[l_lIdX].stElt[l_lPositYHashKey].COMPTE_DODGEHashKey != -1)
		{
			// Free Position not found. Check Duplicate Key
			if (strcmp(COMPTE_DODGEHashArray[l_lIdX].stElt[l_lPositYHashKey].strCOMPTE_DODGE, RefRcaCptFile_Struct.strCOMPTE_DODGE) == 0)
			{
				// printf(" - [AddElementInCOMPTE_DODGEHashKeyTable] - Duplicate Key : Key %s already added in Hash Table\n", RefRcaCptFile_Struct.strCOMPTE_DODGE);
				l_lIdX = HASH_DODGE_ARRAY_SIZE;
			}
		}
		else
		{
			// Free Position found. Add COMPTE_DODGE HaskKey in COMPTE_DODGEHashArray Table
			COMPTE_DODGEHashArray[l_lIdX].stElt[l_lPositYHashKey].COMPTE_DODGEHashKey = l_llHashKey;
			strcpy(COMPTE_DODGEHashArray[l_lIdX].stElt[l_lPositYHashKey].strCOMPTE_DODGE, RefRcaCptFile_Struct.strCOMPTE_DODGE);
			strcpy(COMPTE_DODGEHashArray[l_lIdX].stElt[l_lPositYHashKey].strTOP_BILAN, RefRcaCptFile_Struct.strTOP_BILAN);
			switch (RefRcaCptFile_Struct.strTOP_BILAN[0])
			{
				case 'H'	:	memcpy(COMPTE_DODGEHashArray[l_lIdX].stElt[l_lPositYHashKey].strHB_IMPUTATION, "HB", CD_TYPIMP_FIELD_LENGTH);
								break;
				default		:	memcpy(COMPTE_DODGEHashArray[l_lIdX].stElt[l_lPositYHashKey].strHB_IMPUTATION, "BR", CD_TYPIMP_FIELD_LENGTH);
								break;
			}
			strcpy(COMPTE_DODGEHashArray[l_lIdX].stElt[l_lPositYHashKey].strTOP_INT_EXT, RefRcaCptFile_Struct.strTOP_INT_EXT);
			strcpy(COMPTE_DODGEHashArray[l_lIdX].stElt[l_lPositYHashKey].strTVA, RefRcaCptFile_Struct.strTVA);
			l_lIdX = HASH_DODGE_ARRAY_SIZE;
		}
	}
}

/*
 * =============================================================================
 *                   Add CURRENCY HaskKey in CURRENCYHashArray Table
 * =============================================================================
*/
void AddElementInCURRENCYHashKeyTable ()
{
	long		l_lPositYHashKey	= 0;
	long		l_lIdX				= 0;
	long long   l_llHashKey			= 0;

	l_lPositYHashKey = BuildHashKey (RefCurrencyFile_Struct.strCURRENCY_CD, HASH_CURRENCY_ARRAY_SIZE, &l_llHashKey);

	for (l_lIdX = 0; l_lIdX < HASH_CURRENCY_ARRAY_SIZE; l_lIdX++)
	{
		if (CURRENCYHashArray[l_lIdX].stElt[l_lPositYHashKey].CURRENCYHashKey != -1)
		{
			// Free Position not found. Check Duplicate Key
			if (strcmp(CURRENCYHashArray[l_lIdX].stElt[l_lPositYHashKey].strCURRENCY_CD, RefCurrencyFile_Struct.strCURRENCY_CD) == 0)
			{
				// printf(" - [AddElementInCURRENCYHashKeyTable] - Duplicate Key : Key %s already added in Hash Table\n", RefCurrencyFile_Struct.strCURRENCY_CD);
				l_lIdX = HASH_CURRENCY_ARRAY_SIZE;
			}
		}
		else
		{
			// Free Position found. Add CURRENCY HaskKey in CURRENCYHashArray Table
			CURRENCYHashArray[l_lIdX].stElt[l_lPositYHashKey].CURRENCYHashKey = l_llHashKey;
			strcpy(CURRENCYHashArray[l_lIdX].stElt[l_lPositYHashKey].strCURRENCY_CD, RefCurrencyFile_Struct.strCURRENCY_CD);
			strcpy(CURRENCYHashArray[l_lIdX].stElt[l_lPositYHashKey].strDECIMAL_POS, RefCurrencyFile_Struct.strDECIMAL_POS);
			l_lIdX = HASH_CURRENCY_ARRAY_SIZE;
		}
	}
}

/*
 * =============================================================================
 *                   Add LOT HaskKey in LOTHashArray Table
 * =============================================================================
*/
void AddElementInLOTHashKeyTable (const char *i_strAPPLI_EMET, const char *i_strID_LOT, long *o_lPositXHashKey, long *o_lPositYHashKey)
{
	char		l_strAPPLI_EMET_ID_LOT[20 + 1];
	long		l_lPositYHashKey	= 0;
	long		l_lIdX				= 0;
	long long   l_llHashKey			= 0;

	// Build Hash Key using APPLI_EMET and ID_LOT
	memcpy(l_strAPPLI_EMET_ID_LOT, i_strAPPLI_EMET, strlen(i_strAPPLI_EMET));
	memcpy(l_strAPPLI_EMET_ID_LOT + (int)strlen(i_strAPPLI_EMET), i_strID_LOT, strlen(i_strID_LOT));
	l_strAPPLI_EMET_ID_LOT[strlen(i_strAPPLI_EMET) + strlen(i_strID_LOT)] = '\0';
	
	l_lPositYHashKey = BuildHashKey (i_strID_LOT, HASH_LOT_ARRAY_SIZE, &l_llHashKey);

	for (l_lIdX = 0; l_lIdX < HASH_LOT_ARRAY_SIZE; l_lIdX++)
	{
		if (LOTHashArray[l_lIdX].stElt[l_lPositYHashKey].LOTHashKey != -1)
		{
			// Free Position not found. Key already added in Hash Table : Increment iID_ECRITU.
			if (strcmp(LOTHashArray[l_lIdX].stElt[l_lPositYHashKey].strAPPLI_EMET_ID_LOT, l_strAPPLI_EMET_ID_LOT) == 0)
			{
				// printf(" - [AddElementInLOTHashKeyTable] - Key %s already added in Hash Table\n", l_strAPPLI_EMET_ID_LOT);
				if (LOTHashArray[l_lIdX].stElt[l_lPositYHashKey].iID_ECRITU < MAX_ID_ECRITU)
				{
					LOTHashArray[l_lIdX].stElt[l_lPositYHashKey].iID_ECRITU++;
				}
				else
				{
					LOTHashArray[l_lIdX].stElt[l_lPositYHashKey].iID_ECRITU = 1;
				}
				*o_lPositXHashKey = l_lIdX;
				*o_lPositYHashKey = l_lPositYHashKey;
				l_lIdX = HASH_LOT_ARRAY_SIZE;
			}
		}
		else
		{
			// Free Position found. Add LOTHaskKey in LOTHashArray Table
			LOTHashArray[l_lIdX].stElt[l_lPositYHashKey].LOTHashKey = l_llHashKey;
			strcpy(LOTHashArray[l_lIdX].stElt[l_lPositYHashKey].strAPPLI_EMET_ID_LOT, l_strAPPLI_EMET_ID_LOT);
			strcpy(LOTHashArray[l_lIdX].stElt[l_lPositYHashKey].strAPPLI_EMET, i_strAPPLI_EMET);
			strcpy(LOTHashArray[l_lIdX].stElt[l_lPositYHashKey].strID_LOT, i_strID_LOT);
			LOTHashArray[l_lIdX].stElt[l_lPositYHashKey].iLOT_NUM = iLastLOT_NUM_USED + 1;
			iLastLOT_NUM_USED++;
			LOTHashArray[l_lIdX].stElt[l_lPositYHashKey].iID_ECRITU = 1;
			*o_lPositXHashKey = l_lIdX;
			*o_lPositYHashKey = l_lPositYHashKey;
			l_lIdX = HASH_LOT_ARRAY_SIZE;
		}
	}
}

/*
 * =============================================================================
 *                 Find an Element in COMPTE_DODGEHashArray Table
 * =============================================================================
*/
long FindElementInCOMPTE_DODGEHashArrayTable (const char i_strKey[], long *o_lPositXHashKey, long *o_lPositYHashKey)
{
	char 		l_strKey[MAX_FIELD_LENGTH];
	long		l_lPositYHashKey	= 0;
	long		l_lIdX				= 0;
	int  		l_iSign  			= 0;
	long long   l_llHashKey			= 0;

	// Initialize l_strKey
	strcpy(l_strKey, i_strKey);
	// Suppress spaces at the Left and the Right sides of l_strKey 
	Trim(l_strKey);

	// printf(" - [FindElementInCOMPTE_DODGEHashArrayTable] - i_strKey = %s, l_strKey = %s.\n", i_strKey, l_strKey);

	if (strlen(l_strKey) > 0)
	{
		l_lPositYHashKey = BuildHashKey (l_strKey, HASH_DODGE_ARRAY_SIZE, &l_llHashKey);

		// Find l_lIdX Position in COMPTE_DODGEHashArray Table
		for (l_lIdX = 0; l_lIdX < HASH_DODGE_ARRAY_SIZE; l_lIdX++)
		{
			if (strlen(COMPTE_DODGEHashArray[l_lIdX].stElt[l_lPositYHashKey].strCOMPTE_DODGE) > 0)
			{		
				// printf(" - [FindElementInCOMPTE_DODGEHashArrayTable] - COMPTE_DODGEHashArray[%03ld].stElt[%03ld].strCOMPTE_DODGE = %s.\n", l_lIdX, l_lPositYHashKey, COMPTE_DODGEHashArray[l_lIdX].stElt[l_lPositYHashKey].strCOMPTE_DODGE);
				if (memcmp(COMPTE_DODGEHashArray[l_lIdX].stElt[l_lPositYHashKey].strCOMPTE_DODGE, l_strKey, strlen(l_strKey)) == 0)
				{
					// Key found in COMPTE_DODGEHashArray Table
					*o_lPositXHashKey = l_lIdX;
					*o_lPositYHashKey = l_lPositYHashKey;
					return HASH_KEY_FOUND;
				}
			}
			else
			{
				l_lIdX = HASH_DODGE_ARRAY_SIZE;
			}				
		}
		// Key NOT found in COMPTE_DODGEHashArray Table
		printf(" - [FindElementInCOMPTE_DODGEHashArrayTable] - COMPTE_DODGE Key %s NOT FOUND in COMPTE_DODGEHashArray Table\n", l_strKey);
		*o_lPositXHashKey = -1;
		*o_lPositYHashKey = -1;
		return HASH_KEY_NOT_FOUND;
	}
	else
	{
		// Key is only filled by spaces
		// printf(" - [FindElementInCOMPTE_DODGEHashArrayTable] - COMPTE_DODGE Key is Empty\n");
		*o_lPositXHashKey = -1;
		*o_lPositYHashKey = -1;
		return HASH_KEY_NOT_FOUND;
	}
}

/*
 * =============================================================================
 *                 Find an Element in CURRENCYHashArray Table
 * =============================================================================
*/
long FindElementInCURRENCYHashArrayTable (const char i_strKey[], long *o_lPositXHashKey, long *o_lPositYHashKey)
{
	char 		l_strKey[MAX_FIELD_LENGTH];
	long		l_lPositYHashKey	= 0;
	long		l_lIdX				= 0;
	int  		l_iSign  			= 0;
	long long   l_llHashKey			= 0;

	// Initialize l_strKey
	strcpy(l_strKey, i_strKey);

	// Suppress spaces at the Left and the Right sides of l_strKey
	Trim(l_strKey);
	
	if (strlen(l_strKey) > 0)
	{
		l_lPositYHashKey = BuildHashKey (l_strKey, HASH_CURRENCY_ARRAY_SIZE, &l_llHashKey);

		// Find l_lIdX Position in CURRENCYHashArray Table
		for (l_lIdX = 0; l_lIdX < HASH_CURRENCY_ARRAY_SIZE; l_lIdX++)
		{
			if (strcmp(CURRENCYHashArray[l_lIdX].stElt[l_lPositYHashKey].strCURRENCY_CD, l_strKey) == 0)
			{
				// Key found in CURRENCYHashArray Table
				*o_lPositXHashKey = l_lIdX;
				*o_lPositYHashKey = l_lPositYHashKey;
				return HASH_KEY_FOUND;
			}
		}
		// Key not found in CURRENCYHashArray Table
		// printf(" - [FindElementInCURRENCYHashArrayTable] - CURRENCY Key %s NOT FOUND in CURRENCYHashArray Table\n", l_strKey);
		*o_lPositXHashKey = -1;
		*o_lPositYHashKey = -1;
		return HASH_KEY_NOT_FOUND;
	}
	else
	{
		// Key is only filled by spaces
		// printf(" - [FindElementInCURRENCYHashArrayTable] - CURRENCY Key is Empty\n");
		*o_lPositXHashKey = -1;
		*o_lPositYHashKey = -1;
		return HASH_KEY_NOT_FOUND;
	}
}

/*
 * =============================================================================
 *                 Find an Element in LOTHashArray Table
 * =============================================================================
*/
long FindElementInLOTHashArrayTable (const char i_strKey[], long *o_lPositXHashKey, long *o_lPositYHashKey)
{
	char 		l_strKey[MAX_FIELD_LENGTH];
	long		l_lPositYHashKey	= 0;
	long		l_lIdX				= 0;
	int  		l_iSign  			= 0;
	long long   l_llHashKey			= 0;

	// Initialize l_strKey
	strcpy(l_strKey, i_strKey);

	// Suppress spaces at the Left and the Right sides of l_strKey
	Trim(l_strKey);
	
	if (strlen(l_strKey) > 0)
	{
		l_lPositYHashKey = BuildHashKey (l_strKey, HASH_LOT_ARRAY_SIZE, &l_llHashKey);

		// Find l_lIdX Position in LOTHashArray Table
		for (l_lIdX = 0; l_lIdX < HASH_LOT_ARRAY_SIZE; l_lIdX++)
		{
			if (strcmp(LOTHashArray[l_lIdX].stElt[l_lPositYHashKey].strAPPLI_EMET_ID_LOT, l_strKey) == 0)
			{
				// Key found in LOTHashArray Table
				*o_lPositXHashKey = l_lIdX;
				*o_lPositYHashKey = l_lPositYHashKey;
				return HASH_KEY_FOUND;
			}
		}
		// Key not found in LOTHashArray Table
		// printf(" - [FindElementInLOTHashArrayTable] - LOT Key %s NOT FOUND in LOTHashArray Table\n", l_strKey);
		*o_lPositXHashKey = -1;
		*o_lPositYHashKey = -1;
		return HASH_KEY_NOT_FOUND;
	}
	else
	{
		// Key is only filled by spaces
		// printf(" - [FindElementInLOTHashArrayTable] - LOT Key is Empty\n");
		*o_lPositXHashKey = -1;
		*o_lPositYHashKey = -1;
		return HASH_KEY_NOT_FOUND;
	}
}

/*
 * =============================================================================
 *          Find Id Ecriture using Id Lot in IdLotIdEcriture Table
 * =============================================================================
*/
void FindIdLotIdEcriture (const char *i_strAPPLI_EMET, const char *i_strID_LOT, char *o_strLOT_NUM, char *o_strID_ECRITU)
{
	char	l_strAPPLI_EMET_ID_LOT[20 + 1];
	long	l_lIdXPos			= -1;
	long	l_lIdYPos			= -1;

	// printf(" - [FindIdLotIdEcriture] - i_strAPPLI_EMET = %s, i_strID_LOT = %s\n", i_strAPPLI_EMET, i_strID_LOT);
	
	// Find ID_LOT and ID_ECRI in LOTHashArray Table using APPLI_EMET and ID_LOT
	memcpy(l_strAPPLI_EMET_ID_LOT, i_strAPPLI_EMET, strlen(i_strAPPLI_EMET));
	memcpy(l_strAPPLI_EMET_ID_LOT + (int)strlen(i_strAPPLI_EMET), i_strID_LOT, strlen(i_strID_LOT));
	l_strAPPLI_EMET_ID_LOT[strlen(i_strAPPLI_EMET) + strlen(i_strID_LOT)] = '\0';

	if (FindElementInLOTHashArrayTable (i_strID_LOT, &l_lIdXPos, &l_lIdYPos))
	{
		sprintf(o_strLOT_NUM,  "%017d", LOTHashArray[l_lIdXPos].stElt[l_lIdYPos].iLOT_NUM);
		sprintf(o_strID_ECRITU, "%06d", LOTHashArray[l_lIdXPos].stElt[l_lIdYPos].iID_ECRITU);
	}
	else
	{
		AddElementInLOTHashKeyTable (i_strAPPLI_EMET, i_strID_LOT, &l_lIdXPos, &l_lIdYPos);
		sprintf(o_strLOT_NUM,  "%017d", LOTHashArray[l_lIdXPos].stElt[l_lIdYPos].iLOT_NUM);
		sprintf(o_strID_ECRITU, "%06d", LOTHashArray[l_lIdXPos].stElt[l_lIdYPos].iID_ECRITU);
	}
	// printf(" - [FindIdLotIdEcriture] - i_strAPPLI_EMET = %s, i_strID_LOT = %s, o_strLOT_NUM = %s, o_strID_ECRITU = %s\n", i_strAPPLI_EMET, i_strID_LOT, o_strLOT_NUM, o_strID_ECRITU);
}

/* 
 * =============================================================================
 *  Building Format of REF_RCA_CPT.dat File using REF_RCA_CPT.conf File
 * =============================================================================
 */
int BuildRefRcaCptRecordFormat()
{
	FILE *l_RefRcaCptFile_RecordFormat_Ptr	= NULL;
	char  l_strRefRcaCptFileFormat[REF_RCA_CPT_RECORD_LENGTH];
	char  l_strFullRefRcaCptFormatFileName[MAX_FULL_FILE_NAME_LENGTH];
	char  l_strRefRcaCptField[REF_RCA_CPT_FIELD_NAME_LENGTH];
	int   l_iRefRcaCptFieldNumber			= -1;
	int   l_iRefRcaCptFieldLength			= 0;
	int   l_iFieldNumber					= 0;
	int   l_idx								= 0;

	/* Initializing the table of Fields used to create COMPTE_DODGE Hash Key Table */
	for (l_iFieldNumber = 0; l_iFieldNumber < MAX_FIELD_NUMBER_REF_RCA_CPT; l_iFieldNumber++)
	{
		memset(tabFieldOfRefRcaCptRecord[l_iFieldNumber].strFieldName,   '\0', sizeof(tabFieldOfRefRcaCptRecord[l_iFieldNumber].strFieldName));
		memset(tabFieldOfRefRcaCptRecord[l_iFieldNumber].strFieldFormat, '\0', sizeof(tabFieldOfRefRcaCptRecord[l_iFieldNumber].strFieldFormat));
		tabFieldOfRefRcaCptRecord[l_iFieldNumber].iFieldFormat           = unknown;
		tabFieldOfRefRcaCptRecord[l_iFieldNumber].iFieldLength           = -1;
		tabFieldOfRefRcaCptRecord[l_iFieldNumber].iFieldStartSepPosition = -1;
	}
	
	/* Opening REF_RCA_CPT.conf File : structure of REF_RCA_CPT.dat File */
	strcpy(l_strFullRefRcaCptFormatFileName, strConfigurationDirectory);
	strcat(l_strFullRefRcaCptFormatFileName, "/");
	strcat(l_strFullRefRcaCptFormatFileName, REF_RCA_CPT_FORMAT_FILE_NAME);
	
	printf("Opening %s File ...\n", l_strFullRefRcaCptFormatFileName);
	l_RefRcaCptFile_RecordFormat_Ptr = fopen(l_strFullRefRcaCptFormatFileName, "r");
	if (l_RefRcaCptFile_RecordFormat_Ptr == NULL)
	{
		printf("Error %d : '%s' occurs when opening %s File \n", errno, strerror(errno), l_strFullRefRcaCptFormatFileName);
		return EXIT_ERR;
	}

	/* Reading Records of REF_RCA_CPT.conf File : Each Record gives the Name of a Field of REF_RCA_CPT.dat File Record */
	while (fgets(l_strRefRcaCptField, REF_RCA_CPT_FIELD_NAME_LENGTH, l_RefRcaCptFile_RecordFormat_Ptr) != NULL)
	{
		if (strlen(l_strRefRcaCptField) > 1)
		{
			// Handle only not empty records
			l_iRefRcaCptFieldNumber++;
			if (l_iRefRcaCptFieldNumber < MAX_FIELD_NUMBER_REF_RCA_CPT)
			{
				// memcpy(l_strRefRcaCptFileFormat + l_iRefRcaCptFieldLength, l_strRefRcaCptField, strlen(l_strRefRcaCptField) - 1);
				// l_iRefRcaCptFieldLength = l_iRefRcaCptFieldLength + strlen(l_strRefRcaCptField) - 1;
				// memcpy(l_strRefRcaCptFileFormat + l_iRefRcaCptFieldLength, REF_RCA_CPT_SEPARATOR, 1);
				// l_iRefRcaCptFieldLength = l_iRefRcaCptFieldLength + 1;	
				
				if (memcmp(l_strRefRcaCptField, "COMPTE_DODGE", 12) == 0)
				{
					memcpy(tabFieldOfRefRcaCptRecord[0].strFieldName, l_strRefRcaCptField, 12);
					tabFieldOfRefRcaCptRecord[0].strFieldName[strlen(l_strRefRcaCptField)] = '\0';
					tabFieldOfRefRcaCptRecord[0].iFieldStartSepPosition = l_iRefRcaCptFieldNumber; 
				}
				else
				{
					if (memcmp(l_strRefRcaCptField, "TOP_BILAN", 9) == 0)
					{
						memcpy(tabFieldOfRefRcaCptRecord[1].strFieldName, l_strRefRcaCptField, 9);
						tabFieldOfRefRcaCptRecord[1].strFieldName[strlen(l_strRefRcaCptField)] = '\0';
						tabFieldOfRefRcaCptRecord[1].iFieldStartSepPosition = l_iRefRcaCptFieldNumber; 
					}
					else
					{
						if (memcmp(l_strRefRcaCptField, "TOP_INT_EXT", 11) == 0)
						{
							memcpy(tabFieldOfRefRcaCptRecord[2].strFieldName, l_strRefRcaCptField, 11);
							tabFieldOfRefRcaCptRecord[2].strFieldName[strlen(l_strRefRcaCptField)] = '\0';
							tabFieldOfRefRcaCptRecord[2].iFieldStartSepPosition = l_iRefRcaCptFieldNumber; 
						}
						else
						{
							if (memcmp(l_strRefRcaCptField, "CHAMPS_TVA", 10) == 0)
							{
								memcpy(tabFieldOfRefRcaCptRecord[3].strFieldName, l_strRefRcaCptField, 10);
								tabFieldOfRefRcaCptRecord[3].strFieldName[strlen(l_strRefRcaCptField)] = '\0';
								tabFieldOfRefRcaCptRecord[3].iFieldStartSepPosition = l_iRefRcaCptFieldNumber;
							}
						}					
					}
				}
			}
			else
			{
				// Unexpected Number of Fields in REF_RCA_CPT.conf File
				printf("Number of Fields %d instead of %d in %s File \n", l_iRefRcaCptFieldNumber + 1, MAX_FIELD_NUMBER_REF_RCA_CPT, l_strFullRefRcaCptFormatFileName);
				return EXIT_ERR;
			}
		}
	}
	/*for (l_idx=0; l_idx<4; l_idx++)
	{
		printf(" - [BuildRefRcaCptRecordFormat] - tabFieldOfRefRcaCptRecord[%d].strFieldName           = %s\n", l_idx, tabFieldOfRefRcaCptRecord[l_idx].strFieldName);
		printf(" - [BuildRefRcaCptRecordFormat] - tabFieldOfRefRcaCptRecord[%d].iFieldStartSepPosition = %d\n", l_idx, tabFieldOfRefRcaCptRecord[l_idx].iFieldStartSepPosition); 
	}*/

	// l_strRefRcaCptFileFormat could be represented by the following string : "CODE_COMPTABILITE;NUMERO_COMPTE;CLE_COMPTE;COMPTE_DODGE;LIBELLE_COMPTE;
	// TYPE_COMPTE;TOP_BILAN;TOP_ACTIF_PASSIF;TOP_REGLEMENT;TOP_SENS;TOP_MULTI_DEVISE;TOP_DENOTAGE;TYPE_LETTRAGE;DEVISE;PCEC_DEBIT;PCEC_CREDIT;
	// DATE_CREATION;DATE_MAJ_DCAF;DATE_MAJ_DCMC;TENUE_CID;FAMILLE_CID;CPT_MAITRE;CODE_TENUE;TOP_FRAIS;NUMERO_COMPTE_FIC;NUM_COMPTE_CIDR1;
	// NUM_COMPTE_CIDR2;NUM_COMPTE_CEG;CODE_ANA36;AGENCE_CDR;CLE_AGENCE_COMPTE;TOP_DODGE;TOP_IME;TOP_OSC;TOP_OAX;CODE_SOCIETE;TOP_ACTIF;CHAMPS_TVA;
	// GENE_TVA;TVA;AFF_PRORATA;METHOD_PRORATA;PCCT;TOP_IAS;TOP_INT_EXT;PCIB_IAS_DEBIT;PCIB_IAS_CREDIT;GROUPE_COMPTE;TOSEGVAL;COMPTE_CTP;
	// STATUT_RESULTAT;ID_SUIVI;ASSUJ;TAXATION;ADMISSION"
	// l_strRefRcaCptFileFormat[l_iRefRcaCptFieldLength] = '\0';
	// printf(" - [BuildRefRcaCptRecordFormat] - Structure of %s File given from %s File : \"%s\"\n", REF_RCA_CPT_FILE_NAME, REF_RCA_CPT_FORMAT_FILE_NAME, l_strRefRcaCptFileFormat);

	/* Closing REF_RCA_CPT.conf File */
	printf("Closing %s File ...\n", l_strFullRefRcaCptFormatFileName);
	fclose(l_RefRcaCptFile_RecordFormat_Ptr);
	return EXIT_OK;
}

/* 
 * =============================================================================
 *       Building COMPTE_DODGE Table using data of REF_RCA_CPT.dat File
 * =============================================================================
 */
int BuildCompteDodgeTable()
{
	FILE *l_RefRcaCptFile_Ptr			= NULL;
	char l_strFullRefRcaCptFileName[MAX_FULL_FILE_NAME_LENGTH];
	int  l_iIdx							= 0;
	int  l_iSepPositInRecord			= 0;
	int  l_iNbSepInRecord				= 0;
	int  l_iRecNumber					= 0;
	long COMPTE_DODGEHashKey			= 0;
	long long l_llHashKeyCOMPTE_DODGE	= 0;


	/* Initializing COMPTE_DODGE Hash Key Table */
	InitializeCOMPTE_DODGEHashKeyTable ();
	
	/* Opening REF_RCA_CPT.dat File */
	strcpy(l_strFullRefRcaCptFileName, strConfigurationDirectory);
	strcat(l_strFullRefRcaCptFileName, "/");
	strcat(l_strFullRefRcaCptFileName, strRCA_CPT_FILE_NAME);
	
	printf("Opening %s File ...\n", l_strFullRefRcaCptFileName);
	l_RefRcaCptFile_Ptr = fopen(l_strFullRefRcaCptFileName, "r");
	if (l_RefRcaCptFile_Ptr == NULL)
	{
		printf("Error %d : '%s' occurs when opening %s File \n", errno, strerror(errno), l_strFullRefRcaCptFileName);
		return EXIT_ERR;
	}

	/* Reading Records of REF_RCA_CPT.dat File */
	l_iRecNumber = -1;
	while (fgets(RefRcaCpt_Record, REF_RCA_CPT_RECORD_LENGTH, l_RefRcaCptFile_Ptr) != NULL)
	{
		if (strlen(RefRcaCpt_Record) > 1)
		{
			// Handle only not empty records
			memset(RefRcaCptFile_Struct.strCOMPTE_DODGE, '\0', sizeof(RefRcaCptFile_Struct.strCOMPTE_DODGE));
			memset(RefRcaCptFile_Struct.strTOP_BILAN,    '\0', sizeof(RefRcaCptFile_Struct.strTOP_BILAN));
			memset(RefRcaCptFile_Struct.strTOP_INT_EXT,  '\0', sizeof(RefRcaCptFile_Struct.strTOP_INT_EXT));
			memset(RefRcaCptFile_Struct.strTVA,          '\0', sizeof(RefRcaCptFile_Struct.strTVA));
			
			l_iNbSepInRecord=-1;
			l_iSepPositInRecord=0;
			l_iRecNumber++;
			for(l_iIdx=0; l_iIdx < strlen(RefRcaCpt_Record); l_iIdx++)
			{
				if (memcmp(RefRcaCpt_Record + l_iIdx, REF_RCA_CPT_SEPARATOR, 1) == 0)
				{
					l_iNbSepInRecord++;
					if (l_iNbSepInRecord == tabFieldOfRefRcaCptRecord[0].iFieldStartSepPosition)
					{
						memcpy(RefRcaCptFile_Struct.strCOMPTE_DODGE, RefRcaCpt_Record + l_iSepPositInRecord + 1, l_iIdx - l_iSepPositInRecord - 1);
						RefRcaCptFile_Struct.strCOMPTE_DODGE[l_iIdx - l_iSepPositInRecord - 1] = '\0';
					}
					if (l_iNbSepInRecord == tabFieldOfRefRcaCptRecord[1].iFieldStartSepPosition)
					{
						memcpy(RefRcaCptFile_Struct.strTOP_BILAN, RefRcaCpt_Record + l_iSepPositInRecord + 1, l_iIdx - l_iSepPositInRecord - 1);
						RefRcaCptFile_Struct.strTOP_BILAN[l_iIdx - l_iSepPositInRecord - 1] = '\0';
					}
					if (l_iNbSepInRecord == tabFieldOfRefRcaCptRecord[2].iFieldStartSepPosition)
					{
						if (memcmp(RefRcaCpt_Record + l_iSepPositInRecord + 1, "M", CD_TYPEI_FIELD_LENGTH) == 0)
						{
							memcpy(RefRcaCptFile_Struct.strTOP_INT_EXT, "I", CD_TYPEI_FIELD_LENGTH);
							RefRcaCptFile_Struct.strTOP_INT_EXT[CD_TYPEI_FIELD_LENGTH] = '\0';
						}
						else
						{
							memcpy(RefRcaCptFile_Struct.strTOP_INT_EXT, RefRcaCpt_Record + l_iSepPositInRecord + 1 , l_iIdx - l_iSepPositInRecord - 1);
							RefRcaCptFile_Struct.strTOP_INT_EXT[l_iIdx - l_iSepPositInRecord - 1] = '\0';
						}
					}
					if (l_iNbSepInRecord == tabFieldOfRefRcaCptRecord[3].iFieldStartSepPosition)
					{
						memcpy(RefRcaCptFile_Struct.strTVA, RefRcaCpt_Record + l_iSepPositInRecord + 1 , l_iIdx - l_iSepPositInRecord - 1);
						RefRcaCptFile_Struct.strTVA[l_iIdx - l_iSepPositInRecord - 1] = '\0';
					}					
					l_iSepPositInRecord = l_iIdx;
					// printf(" - [BuildCompteDodgeTable] - Record %04d : Separator No %02d at position %04d\n", l_iRecNumber, l_iNbSepInRecord + 1, l_iSepPositInRecord);
				}
			}
			if (l_iNbSepInRecord + 1 != MAX_SEPARATOR_IN_REF_RCA_CPT)
			{
				RefRcaCpt_Record[strlen(RefRcaCpt_Record) - 1] = '\0';
				printf("Number of separator %d instead of %d in %s File - Record %06d rejected : \"%s\"\n", l_iNbSepInRecord + 1, MAX_SEPARATOR_IN_REF_RCA_CPT, l_strFullRefRcaCptFileName, l_iRecNumber + 1, RefRcaCpt_Record);
			}
			else
			{
				if (strlen(RefRcaCptFile_Struct.strCOMPTE_DODGE) == 0)
				{
					// Record rejected
					RefRcaCpt_Record[strlen(RefRcaCpt_Record) - 1] = '\0';
					printf("COMPTE_DODGE Field NOT FOUND in %s File - Record %06d rejected : \"%s\"\n", l_strFullRefRcaCptFileName, l_iRecNumber + 1, RefRcaCpt_Record);
				}
				else
				{
					// Record taken into account
					// printf(" - [BuildCompteDodgeTable] - %s Record %03d : %s \t\t| %s \t\t| %s \t\t| %s\n", l_strFullRefRcaCptFileName, l_iRecNumber, RefRcaCptFile_Struct.strCOMPTE_DODGE, RefRcaCptFile_Struct.strTOP_BILAN, RefRcaCptFile_Struct.strTOP_INT_EXT, RefRcaCptFile_Struct.strTVA);
					if (strlen(RefRcaCptFile_Struct.strCOMPTE_DODGE) > 0)
					{
						AddElementInCOMPTE_DODGEHashKeyTable ();
					}
				}
			}
		}
	}
	/* Closing REF_RCA_CPT.dat File */
	printf("Closing %s File ...\n", l_strFullRefRcaCptFileName);
	fclose(l_RefRcaCptFile_Ptr);
	return EXIT_OK;
}

/* 
 * =============================================================================
 *  Building Format of REF_CURRENCY.dat File using REF_CURRENCY.conf File
 * =============================================================================
 */
int BuildRefCurrencyRecordFormat()
{
	FILE *l_CurrencyFile_RecordFormat_Ptr	= NULL;
	char l_strFullRefCurrencyFormatFileName[MAX_FULL_FILE_NAME_LENGTH];
	char l_strFieldName[FIELD_NAME_LENGTH];
	int  l_iRefCurrencyFieldNumber			= -1;
	int  l_iFieldNumber						= 0;

	/* Initializing the table of Fields of Currency Format File */
	for (l_iFieldNumber = 0; l_iFieldNumber < MAX_FIELD_NUMBER_CURRENCY; l_iFieldNumber++)
	{
		memset(tabFieldOfRefCurrencyRecord[l_iFieldNumber].strFieldName,   '\0', sizeof(tabFieldOfRefCurrencyRecord[l_iFieldNumber].strFieldName));
		memset(tabFieldOfRefCurrencyRecord[l_iFieldNumber].strFieldFormat, '\0', sizeof(tabFieldOfRefCurrencyRecord[l_iFieldNumber].strFieldFormat));
		tabFieldOfRefCurrencyRecord[l_iFieldNumber].iFieldFormat           = unknown;
		tabFieldOfRefCurrencyRecord[l_iFieldNumber].iFieldLength           = -1;
		tabFieldOfRefCurrencyRecord[l_iFieldNumber].iFieldStartSepPosition = -1;
	}
	
	/* Opening REF_CURRENCY.conf File : structure of REF_CURRENCY.dat File */
	strcpy(l_strFullRefCurrencyFormatFileName, strConfigurationDirectory);
	strcat(l_strFullRefCurrencyFormatFileName, "/");
	strcat(l_strFullRefCurrencyFormatFileName, REF_CURRENCY_FORMAT_FILE_NAME);
	
	printf("Opening %s File ...\n", l_strFullRefCurrencyFormatFileName);
	l_CurrencyFile_RecordFormat_Ptr = fopen(l_strFullRefCurrencyFormatFileName, "r");
	if (l_CurrencyFile_RecordFormat_Ptr == NULL)
	{
		printf("Error %d : '%s' occurs when opening %s File \n", errno, strerror(errno), l_strFullRefCurrencyFormatFileName);
		return EXIT_ERR;
	}

	/* Reading Records of REF_CURRENCY.conf File : Each Record gives the Characteristics of a Field of the Currency File Record */
	while (fgets((char*) &Currency_File_Record_Format_Struct, MAX_CURRENCY_FORMAT_REC_LENGTH, l_CurrencyFile_RecordFormat_Ptr) != NULL)
	{
		if (strlen(Currency_File_Record_Format_Struct.CurrencyFormat_Record) > 1)
		{
			// Handle only not empty records
			l_iRefCurrencyFieldNumber++;
			if (l_iRefCurrencyFieldNumber < MAX_FIELD_NUMBER_CURRENCY)
			{
				strcpy(l_strFieldName, strtok(Currency_File_Record_Format_Struct.CurrencyFormat_Record, REF_CURRENCY_SEPARATOR));
				if (memcmp(l_strFieldName, "CURRENCY_CD", 11) == 0)
				{
					if (l_iRefCurrencyFieldNumber == 0)
					{
						strcpy(tabFieldOfRefCurrencyRecord[0].strFieldName, l_strFieldName);
						strcpy(tabFieldOfRefCurrencyRecord[0].strFieldFormat, strtok(NULL, REF_CURRENCY_SEPARATOR));
						tabFieldOfRefCurrencyRecord[0].iFieldFormat = whichOutputFormat(tabFieldOfRefCurrencyRecord[0].strFieldFormat);
						tabFieldOfRefCurrencyRecord[0].iFieldLength = atoi(strtok(NULL, REF_CURRENCY_SEPARATOR));
						tabFieldOfRefCurrencyRecord[0].iFieldStartSepPosition = tabFieldOfRefCurrencyRecord[0].iFieldLength;
					}
					else
					{
						// Unexpected Position of CURRENCY_CD in REF_CURRENCY.conf File
						printf("Unexpected Position of CURRENCY_CD in %s File \n", l_strFullRefCurrencyFormatFileName);
						return EXIT_ERR;
					}
				}
				else
				{
					if (memcmp(l_strFieldName, "DECIMAL_POS", 11) == 0)
					{
						if (l_iRefCurrencyFieldNumber == 1)
						{
							strcpy(tabFieldOfRefCurrencyRecord[1].strFieldName, l_strFieldName);
							strcpy(tabFieldOfRefCurrencyRecord[1].strFieldFormat, strtok(NULL, REF_CURRENCY_SEPARATOR));
							tabFieldOfRefCurrencyRecord[1].iFieldFormat = whichOutputFormat(tabFieldOfRefCurrencyRecord[1].strFieldFormat);
							tabFieldOfRefCurrencyRecord[1].iFieldLength = atoi(strtok(NULL, REF_CURRENCY_SEPARATOR));
							tabFieldOfRefCurrencyRecord[1].iFieldStartSepPosition = tabFieldOfRefCurrencyRecord[0].iFieldLength + 1 + tabFieldOfRefCurrencyRecord[1].iFieldLength;
						}
						else
						{
							// Unexpected Position of DECIMAL_POS in REF_CURRENCY.conf File
							printf("Unexpected Position of DECIMAL_POS in %s File \n", l_strFullRefCurrencyFormatFileName);
							return EXIT_ERR;
						}
					}
				}
			}
			else
			{
				// Unexpected Number of Fields in REF_CURRENCY.conf File
				printf("Number of Fields %d instead of %d in %s File \n", l_iRefCurrencyFieldNumber + 1, MAX_FIELD_NUMBER_CURRENCY, l_strFullRefCurrencyFormatFileName);
				return EXIT_ERR;
			}
		}
	}
	/* Closing REF_CURRENCY.conf File */
	printf("Closing %s File ...\n", l_strFullRefCurrencyFormatFileName);
	fclose(l_CurrencyFile_RecordFormat_Ptr);
	return EXIT_OK;
}

/*
 * =============================================================================
 *   Building CURRENCY Table using data of REF_CURRENCY.dat File
 * =============================================================================
 */
int BuildCurrencyTable()
{
	FILE *l_CurrencyFile_Ptr		= NULL;
	char l_strFullRefCurrencyFileName[MAX_FULL_FILE_NAME_LENGTH];
	int  l_iIdx						= 0;
	int  l_iNbSepInRecord			= 0;
	int  l_iRecNumber				= -1;
	int  l_iRecRejected             = -1;
	long CURRENCYHashKey			= 0;
	long long l_llHashKeyCURRENCY	= 0;

	/* Initializing CURRENCY Hash Key Table */
	InitializeCURRENCYHashKeyTable ();
	
	/* Opening REF_CURRENCY.dat File */
	strcpy(l_strFullRefCurrencyFileName, strConfigurationDirectory);
	strcat(l_strFullRefCurrencyFileName, "/");
	strcat(l_strFullRefCurrencyFileName, REF_CURRENCY_FILE_NAME);
	
	printf("Opening %s File ...\n", l_strFullRefCurrencyFileName);
	l_CurrencyFile_Ptr = fopen(l_strFullRefCurrencyFileName, "r");
	if (l_CurrencyFile_Ptr == NULL)
	{
		printf("Error %d : '%s' occurs when opening %s File \n", errno, strerror(errno), l_strFullRefCurrencyFileName);
		return EXIT_ERR;
	}

	// Reading Records of REF_CURRENCY.dat File : Each Record gives the "CURRENCY_CD" and "DECIMAL_POS" Data
	while (fgets(RefCurrency_Record, REF_CURRENCY_RECORD_LENGTH, l_CurrencyFile_Ptr) != NULL)
	{
		l_iRecNumber++;
		if (strlen(RefCurrency_Record) > 1)
		{
			// Handle only not empty records
			l_iRecRejected = 0;
			if (strlen(RefCurrency_Record) == tabFieldOfRefCurrencyRecord[0].iFieldLength + 1 + tabFieldOfRefCurrencyRecord[1].iFieldLength + 1) // 1 + 1 : separator + carriage return
			{
				// Correct Record Length
				memset(RefCurrencyFile_Struct.strCURRENCY_CD, '\0', sizeof(RefCurrencyFile_Struct.strCURRENCY_CD));
				memset(RefCurrencyFile_Struct.strDECIMAL_POS, '\0', sizeof(RefCurrencyFile_Struct.strDECIMAL_POS));

				l_iNbSepInRecord=-1;

				for(l_iIdx=0; l_iIdx < strlen(RefCurrency_Record); l_iIdx++)
				{
					if (memcmp(RefCurrency_Record + l_iIdx, REF_CURRENCY_SEPARATOR, 1) == 0)
					{
						l_iNbSepInRecord++;
						if ((l_iNbSepInRecord == 0) && (l_iIdx == tabFieldOfRefCurrencyRecord[0].iFieldStartSepPosition))
						{
							// Correct Position of REF_CURRENCY_SEPARATOR
							memcpy(RefCurrencyFile_Struct.strCURRENCY_CD, RefCurrency_Record, tabFieldOfRefCurrencyRecord[0].iFieldLength);
							RefCurrencyFile_Struct.strCURRENCY_CD[tabFieldOfRefCurrencyRecord[0].iFieldLength] = '\0';
							memcpy(RefCurrencyFile_Struct.strDECIMAL_POS, RefCurrency_Record + l_iIdx + 1, tabFieldOfRefCurrencyRecord[1].iFieldLength);
							RefCurrencyFile_Struct.strDECIMAL_POS[tabFieldOfRefCurrencyRecord[1].iFieldLength] = '\0';
						}
						else
						{
							// Unexpected Position of separator
							RefCurrency_Record[strlen(RefCurrency_Record) - 1] = '\0';
							printf("Unexpected Position of separator %d instead of %d in %s File - Record %06d rejected : \"%s\"\n", l_iIdx + 1, tabFieldOfRefCurrencyRecord[0].iFieldStartSepPosition + 1, l_strFullRefCurrencyFileName, l_iRecNumber + 1, RefCurrency_Record);
							l_iRecRejected = 1;
						}
						l_iIdx = strlen(RefCurrency_Record);
					}
				}
				if (l_iRecRejected == 0)
				{
					if (l_iNbSepInRecord + 1 != MAX_SEPARATOR_IN_REF_CURRENCY)
					{
						// Unexpected Number of separators
						RefCurrency_Record[strlen(RefCurrency_Record) - 1] = '\0';
						printf("Unexpected Number of separators %d instead of %d in %s File - Record %06d rejected : \"%s\"\n", l_iNbSepInRecord + 1, MAX_SEPARATOR_IN_REF_CURRENCY, l_strFullRefCurrencyFileName, l_iRecNumber + 1, RefCurrency_Record);
					}
					else
					{
						if (! isdigit(RefCurrencyFile_Struct.strDECIMAL_POS[0]))
						{
							// Unexpected Value of DECIMAL_POS
							RefCurrency_Record[strlen(RefCurrency_Record) - 1] = '\0';
							printf("Unexpected Value of DECIMAL_POS %c in %s File - Record %06d rejected : \"%s\"\n", RefCurrencyFile_Struct.strDECIMAL_POS[0], l_strFullRefCurrencyFileName, l_iRecNumber + 1, RefCurrency_Record);
						}
						else
						{
							// Record taken into account
							// printf(" - [BuildCurrencyTable] - %s Record %03d : %s \t\t| %s \n", l_strFullRefCurrencyFileName, l_iRecNumber + 1, RefCurrencyFile_Struct.strCURRENCY_CD, RefCurrencyFile_Struct.strDECIMAL_POS);
							if (strlen(RefCurrencyFile_Struct.strCURRENCY_CD) > 0)
							{
								AddElementInCURRENCYHashKeyTable ();
							}
						}
					}
				}
			}
			else
			{
				// Unexpected Record Length
				RefCurrency_Record[strlen(RefCurrency_Record) - 1] = '\0';
				printf("Unexpected Record Length %d instead of %d in %s File - Record %06d rejected : \"%s\"\n", strlen(RefCurrency_Record), tabFieldOfRefCurrencyRecord[0].iFieldLength + 1 + tabFieldOfRefCurrencyRecord[1].iFieldLength, l_strFullRefCurrencyFileName, l_iRecNumber + 1, RefCurrency_Record);
			}
		}
	}
	/* Closing REF_CURRENCY.dat File */
	printf("Closing %s File ...\n", l_strFullRefCurrencyFileName);
	fclose(l_CurrencyFile_Ptr);
	return EXIT_OK;
}

/* 
 * =============================================================================
 *                       Convert Input Field
 * =============================================================================
 */
void Convert_InputField (const char *i_strInputField, const int *i_iIdx, char *o_strOutputField)
{
	char l_strInputField[MAX_FIELD_LENGTH];
	char l_strOutputField[MAX_FIELD_LENGTH];
	char l_strDecimalNumber[2];
	int  l_iIdx   			= 0;
	int  l_iSign  			= 0;
	int  l_iDecimalNumber   = 0;
	
	// Initialize l_iIdx and l_strInputField
	l_iIdx = *i_iIdx;
	strcpy(l_strInputField, i_strInputField);
	// printf(" - [Convert_InputField] - l_strInputField = %s\n", l_strInputField);
	// printf(" - [Convert_InputField] - tabFieldOfRecord[%d].iFieldFormat = %d\n", l_iIdx, tabFieldOfRecord[l_iIdx].iFieldFormat);
	// Check if the Input Field must be converted
	switch (tabFieldOfRecord[l_iIdx].iFieldFormat)
	{
		case skip		:	// Original Value without Formating
							memcpy(o_strOutputField, l_strInputField, tabFieldOfRecord[l_iIdx].iFieldLengthInput);
							break;

		case charED		:	// Right space filled
							Trim(l_strInputField);
							memset(o_strOutputField, ' ', tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
							memcpy(o_strOutputField, l_strInputField, strlen(l_strInputField));
							break;

		case charEG		:	// Left  space filled
							Trim(l_strInputField);
							memset(o_strOutputField, ' ', tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
							memcpy(o_strOutputField + tabFieldOfRecord[l_iIdx].iFieldLengthOutput - strlen(l_strInputField), l_strInputField, strlen(l_strInputField));
							break;
							
		case entierEG	:	// Unsigned Numeric with Left spaces
							if (isNumeric(i_strInputField, l_strInputField, &l_iSign))
							{
								memset(o_strOutputField, ' ', tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
								memcpy(o_strOutputField + tabFieldOfRecord[l_iIdx].iFieldLengthOutput - strlen(l_strInputField), l_strInputField, strlen(l_strInputField));
							}
							else
							{
								// In this case, the Original Value is returned without Formating
								memcpy(o_strOutputField, l_strInputField, tabFieldOfRecord[l_iIdx].iFieldLengthInput);
								printf(" - [Convert_InputField] - Unexpected Non Numeric Field %s found in Input File\n", l_strInputField);
							}
							break;
		
		case entierZG	:	// Unsigned Numeric with Left zeroes
							if (isNumeric(i_strInputField, l_strInputField, &l_iSign))
							{
								memset(o_strOutputField, '0', tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
								memcpy(o_strOutputField + tabFieldOfRecord[l_iIdx].iFieldLengthOutput - strlen(l_strInputField), l_strInputField, strlen(l_strInputField));
							}
							else
							{
								// In this case, the Original Value is returned without Formating
								memcpy(o_strOutputField, l_strInputField, tabFieldOfRecord[l_iIdx].iFieldLengthInput);
								printf(" - [Convert_InputField] - Unexpected Non Numeric Field %s found in Input File\n", l_strInputField);
							}
							break;
							
		case entierSG	:	// Signed Numeric with Left spaces - The sign is at the Left side
							if (isNumeric(i_strInputField, l_strInputField, &l_iSign))
							{
								memset(o_strOutputField, ' ', tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
								o_strOutputField[0] = SIGN(l_iSign);
								memcpy(o_strOutputField + tabFieldOfRecord[l_iIdx].iFieldLengthOutput - strlen(l_strInputField), l_strInputField, strlen(l_strInputField));
							}
							else
							{
								// In this case, the Original Value is returned without Formating
								memcpy(o_strOutputField, l_strInputField, tabFieldOfRecord[l_iIdx].iFieldLengthInput);
								printf(" - [Convert_InputField] - Unexpected Non Numeric Field %s found in Input File\n", l_strInputField);
							}
							break;
							
		case entierSZG	:	// Signed Numeric with Left zeroes - The sign is at the Left side
							if (isNumeric(i_strInputField, l_strInputField, &l_iSign))
							{
								memset(o_strOutputField, '0', tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
								o_strOutputField[0] = SIGN(l_iSign);
								memcpy(o_strOutputField + tabFieldOfRecord[l_iIdx].iFieldLengthOutput - strlen(l_strInputField), l_strInputField, strlen(l_strInputField));
							}
							else
							{
								// In this case, the Original Value is returned without Formating
								memcpy(o_strOutputField, l_strInputField, tabFieldOfRecord[l_iIdx].iFieldLengthInput);
								printf(" - [Convert_InputField] - Unexpected Non Numeric Field %s found in Input File\n", l_strInputField);
							}
							break;
							
		case entierSD	:	// Signed Numeric with Left spaces - The sign is at the Right side
							if (isNumeric(i_strInputField, l_strInputField, &l_iSign))
							{
								memset(o_strOutputField, ' ', tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
								memcpy(o_strOutputField + tabFieldOfRecord[l_iIdx].iFieldLengthOutput - strlen(l_strInputField) - 1, l_strInputField, strlen(l_strInputField));
								o_strOutputField[tabFieldOfRecord[l_iIdx].iFieldLengthOutput - 1] = SIGN(l_iSign);
							}
							else
							{
								// In this case, the Original Value is returned without Formating
								memcpy(o_strOutputField, l_strInputField, tabFieldOfRecord[l_iIdx].iFieldLengthInput);
								printf(" - [Convert_InputField] - Unexpected Non Numeric Field %s found in Input File\n", l_strInputField);
							}
							break;
							
		case entierSZD	:	// Signed Numeric with Left zeroes - The sign is at the Right side
							if (isNumeric(i_strInputField, l_strInputField, &l_iSign))
							{
								memset(o_strOutputField, '0', tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
								memcpy(o_strOutputField + tabFieldOfRecord[l_iIdx].iFieldLengthOutput - strlen(l_strInputField) - 1, l_strInputField, strlen(l_strInputField));
								o_strOutputField[tabFieldOfRecord[l_iIdx].iFieldLengthOutput - 1] = SIGN(l_iSign);
							}
							else
							{
								// In this case, the Original Value is returned without Formating
								memcpy(o_strOutputField, l_strInputField, tabFieldOfRecord[l_iIdx].iFieldLengthInput);
								printf(" - [Convert_InputField] - Unexpected Non Numeric Field %s found in Input File\n", l_strInputField);
							}
							break;

		case amount3DEC	:	// Amount with the following Format : Sign, Amount and Number of decimals
							// This amount will be updated (with 3 Decimals) in CorrectFormatAmount Function
							if (isValidInputAmount(l_strInputField, l_strOutputField, &l_iSign, &l_iDecimalNumber))
							{
								// printf(" - [Convert_InputField] - l_strInputField = %s, l_strOutputField = %s, l_iSign = %d, l_iDecimalNumber = %d.\n", l_strInputField,l_strOutputField, l_iSign, l_iDecimalNumber);
								memset(o_strOutputField, '0', tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
								o_strOutputField[0]  = SIGN(l_iSign);
								memcpy(o_strOutputField + tabFieldOfRecord[l_iIdx].iFieldLengthOutput - strlen(l_strOutputField) - 1, l_strOutputField, strlen(l_strOutputField));
								sprintf(l_strDecimalNumber, "%d", l_iDecimalNumber);
								memcpy(o_strOutputField + tabFieldOfRecord[l_iIdx].iFieldLengthOutput - 1, l_strDecimalNumber, 1);
							}
							else
							{
								// The Input Amount is Invalid
								memcpy(o_strOutputField, l_strInputField, tabFieldOfRecord[l_iIdx].iFieldLengthInput);
								printf(" - [Convert_InputField] - Invalid Amount %s found in Input File\n", l_strInputField);
							}
							break;

		default			:	// If unknown Format, we suppose that the Original Value is returned without Formating
							memcpy(o_strOutputField, l_strInputField, tabFieldOfRecord[l_iIdx].iFieldLengthInput);
							printf(" - [Convert_InputField] - Unknown Format %s for Field %s in Input File\n", tabFieldOfRecord[l_iIdx].iFieldFormat, tabFieldOfRecord[l_iIdx].strFieldName);
							break;
	}
}

/* 
 * =============================================================================
 *                       Create Output Record
 * =============================================================================
 */
int Create_Output_Record(const char *i_InputRecord, char *o_OutputRecord)
{
	char l_strIdLot[17 + 1];
	char l_strDAT_OPE[4 + 1];
	char l_strAppliEmet[3 + 1];
	char l_strLotNum[17 + 1];
	char l_strIdEcriture[6 + 1];
	char l_strInputField[MAX_FIELD_LENGTH];
	char l_strOutputField[MAX_FIELD_LENGTH];
	char l_strHB_IMPUTATION[CD_TYPIMP_FIELD_LENGTH + 1];
	char l_strTOP_INT_EXT[CD_TYPEI_FIELD_LENGTH + 1];
	char l_strTVA[CD_TVA_APP_FIELD_LENGTH + 1];
	char l_strDEV_IMP[3 + 1];
	char l_strDEV_GES[3 + 1];
	char l_strDEV_CTP[3 + 1];
	char l_strDEV_IMP_DECIMAL_POS[1 + 1];
	char l_strDEV_GES_DECIMAL_POS[1 + 1];
	char l_strDEV_CTP_DECIMAL_POS[1 + 1];
	char l_strMAI_MNT_IMP[20 + 1];
	char l_strMAI_MNT_GES[20 + 1];
	char l_strMAI_MNT_NOM[20 + 1];
	char l_strOutputAmount[20 + 1];
	int  l_iIdx   			= 0;
	int  l_iSign  			= 0;
	long l_lPositXHashKey	= -1;
	long l_lPositYHashKey	= -1;

	// Initialize l_strInputField and l_strOutputField
	memset(l_strInputField,  ' ', MAX_FIELD_LENGTH);
	memset(l_strOutputField, ' ', MAX_FIELD_LENGTH);
	
	// Set Default Values for HB_IMPUTATION, TOP_INT_EXT and TVA
	memcpy(l_strHB_IMPUTATION, "BR", CD_TYPIMP_FIELD_LENGTH);
	l_strHB_IMPUTATION[CD_TYPIMP_FIELD_LENGTH] = '\0';
	memcpy(l_strTOP_INT_EXT, "E", CD_TYPEI_FIELD_LENGTH);
	l_strTOP_INT_EXT[CD_TYPEI_FIELD_LENGTH] = '\0';
	memcpy(l_strTVA, "  ", CD_TVA_APP_FIELD_LENGTH);
	l_strTVA[CD_TVA_APP_FIELD_LENGTH] = '\0';
	
	while (strlen(tabFieldOfRecord[l_iIdx].strFieldName) > 0)
	{
		memcpy(l_strInputField, i_InputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosInput, tabFieldOfRecord[l_iIdx].iFieldLengthInput);
		l_strInputField[tabFieldOfRecord[l_iIdx].iFieldLengthInput] = '\0';
		Convert_InputField (l_strInputField, &l_iIdx, l_strOutputField);
		memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
		// Currency : MAI_DEV_IMP
		if (strcmp(tabFieldOfRecord[l_iIdx].strFieldName, "MAI_DEV_IMP") == 0)
		{
			strcpy(l_strDEV_IMP, l_strInputField);
			if (strcmp(l_strDEV_IMP, EMPTY_CURRENCY) == 0)
			{
				// Currency Field Empty
				l_strDEV_IMP_DECIMAL_POS[0] = '3';
				l_strDEV_IMP_DECIMAL_POS[1] = '\0';
			}
			else
			{
				if (FindElementInCURRENCYHashArrayTable(l_strInputField, &l_lPositXHashKey, &l_lPositYHashKey))
				{
					strcpy(l_strDEV_IMP_DECIMAL_POS, CURRENCYHashArray[l_lPositXHashKey].stElt[l_lPositYHashKey].strDECIMAL_POS);
				}
				else
				{
					// Currency not found in CURRENCYHashArray Table
					l_strDEV_IMP_DECIMAL_POS[0] = '3';
					l_strDEV_IMP_DECIMAL_POS[1] = '\0';
					printf("Currency NOT FOUND ............ : %s. Default Decimal Number = %s\n", l_strDEV_IMP, l_strDEV_IMP_DECIMAL_POS);
				}
			}
		}
		// Amount : MAI_MNT_IMP
		if (strcmp(tabFieldOfRecord[l_iIdx].strFieldName, "MAI_MNT_IMP") == 0)
		{
			memcpy(l_strMAI_MNT_IMP, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
			l_strMAI_MNT_IMP[tabFieldOfRecord[l_iIdx].iFieldLengthOutput] = '\0';
			CorrectFormatAmount(l_strMAI_MNT_IMP, l_strDEV_IMP_DECIMAL_POS, l_strOutputAmount);
			// printf(" - [Create_Output_Record] - l_strMAI_MNT_IMP = %s, l_strDEV_IMP_DECIMAL_POS = %s, l_strOutputAmount= %s\n", l_strMAI_MNT_IMP, l_strDEV_IMP_DECIMAL_POS, l_strOutputAmount);
			memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputAmount, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
		}		
		// Currency of Management : MAI_DEV_GES
		if (strcmp(tabFieldOfRecord[l_iIdx].strFieldName, "MAI_DEV_GES") == 0)
		{
			strcpy(l_strDEV_GES, l_strInputField);
			if (strcmp(l_strDEV_GES, EMPTY_CURRENCY) == 0)
			{
				// Currency Field Empty
				l_strDEV_GES_DECIMAL_POS[0] = '3';
				l_strDEV_GES_DECIMAL_POS[1] = '\0';
			}
			else
			{
				if (FindElementInCURRENCYHashArrayTable(l_strInputField, &l_lPositXHashKey, &l_lPositYHashKey))
				{
					strcpy(l_strDEV_GES_DECIMAL_POS, CURRENCYHashArray[l_lPositXHashKey].stElt[l_lPositYHashKey].strDECIMAL_POS);
				}
				else
				{
					// Currency not found in CURRENCYHashArray Table
					l_strDEV_GES_DECIMAL_POS[0] = '3';
					l_strDEV_GES_DECIMAL_POS[1] = '\0';
					printf("Currency NOT FOUND ............ : %s. Default Decimal Number = %s\n", l_strDEV_GES, l_strDEV_GES_DECIMAL_POS);
				}
			}
		}
		// Amount : MAI_MNT_GES
		if (strcmp(tabFieldOfRecord[l_iIdx].strFieldName, "MAI_MNT_GES") == 0)
		{
			memcpy(l_strMAI_MNT_GES, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
			l_strMAI_MNT_GES[tabFieldOfRecord[l_iIdx].iFieldLengthOutput] = '\0';
			CorrectFormatAmount(l_strMAI_MNT_GES, l_strDEV_GES_DECIMAL_POS, l_strOutputAmount);
			// printf(" - [Create_Output_Record] - l_strMAI_MNT_GES = %s, l_strDEV_GES_DECIMAL_POS = %s, l_strOutputAmount= %s\n", l_strMAI_MNT_GES, l_strDEV_GES_DECIMAL_POS, l_strOutputAmount);
			memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputAmount, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
		}		
		// Original Currency of Operation : MAI_DEV_CTP
		if (strcmp(tabFieldOfRecord[l_iIdx].strFieldName, "MAI_DEV_CTP") == 0)
		{
			strcpy(l_strDEV_CTP, l_strInputField);
			if (strcmp(l_strDEV_CTP, EMPTY_CURRENCY) == 0)
			{
				// Currency Field Empty
				l_strDEV_CTP_DECIMAL_POS[0] = '3';
				l_strDEV_CTP_DECIMAL_POS[1] = '\0';
			}
			else
			{
				if (FindElementInCURRENCYHashArrayTable(l_strInputField, &l_lPositXHashKey, &l_lPositYHashKey))
				{
					strcpy(l_strDEV_CTP_DECIMAL_POS, CURRENCYHashArray[l_lPositXHashKey].stElt[l_lPositYHashKey].strDECIMAL_POS);
				}
				else
				{
					// Currency not found in CURRENCYHashArray Table
					l_strDEV_CTP_DECIMAL_POS[0] = '3';
					l_strDEV_CTP_DECIMAL_POS[1] = '\0';
					printf("Currency NOT FOUND ............ : %s. Default Decimal Number = %s\n", l_strDEV_CTP, l_strDEV_CTP_DECIMAL_POS);
				}
			}
		}
		// Amount : MAI_MNT_NOM
		if (strcmp(tabFieldOfRecord[l_iIdx].strFieldName, "MAI_MNT_NOM") == 0)
		{
			memcpy(l_strMAI_MNT_NOM, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
			l_strMAI_MNT_NOM[tabFieldOfRecord[l_iIdx].iFieldLengthOutput] = '\0';
			CorrectFormatAmount(l_strMAI_MNT_NOM, l_strDEV_GES_DECIMAL_POS, l_strOutputAmount);
			// printf(" - [Create_Output_Record] - l_strMAI_MNT_NOM = %s, l_strDEV_GES_DECIMAL_POS = %s, l_strOutputAmount= %s\n", l_strMAI_MNT_NOM, l_strDEV_GES_DECIMAL_POS, l_strOutputAmount);
			memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputAmount, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
		}		
		// Build LOT Identifier using DODGE Account : MAI_CPT_IMP
		// For an efficient use of the LOT Hash Key, we have chosen to write l_strIdLot
		// in the following order : DAT_OPE, l_strHB_IMPUTATION, l_strTOP_INT_EXT, NUM_CRE
		// instead of the order   : l_strHB_IMPUTATION, l_strTOP_INT_EXT, NUM_CRE, DAT_OPE
		// DATE_OPE
		if (strcmp(tabFieldOfRecord[l_iIdx].strFieldName, "MAI_DAT_OPE") == 0)
		{
			memcpy(l_strDAT_OPE, l_strInputField, 4);
			memcpy(l_strIdLot, l_strInputField, DATE_LENGTH);
		}		
		if (strcmp(tabFieldOfRecord[l_iIdx].strFieldName, "MAI_CPT_IMP") == 0)
		{
			// HB_IMPUTATION, TOP_INT_EXT and TVA
			 // printf(" - [Create_Output_Record] - Dodge Account = %s.\n", l_strInputField);
			if (FindElementInCOMPTE_DODGEHashArrayTable(l_strInputField, &l_lPositXHashKey, &l_lPositYHashKey))
			{
				strcpy(l_strHB_IMPUTATION, COMPTE_DODGEHashArray[l_lPositXHashKey].stElt[l_lPositYHashKey].strHB_IMPUTATION);
				strcpy(l_strTOP_INT_EXT, COMPTE_DODGEHashArray[l_lPositXHashKey].stElt[l_lPositYHashKey].strTOP_INT_EXT);
				strcpy(l_strTVA, COMPTE_DODGEHashArray[l_lPositXHashKey].stElt[l_lPositYHashKey].strTVA);
			}
			else
			{
				printf("Dodge Account NOT FOUND ....... : %s\n", l_strInputField);
			}
			memcpy(l_strIdLot + DATE_LENGTH, l_strHB_IMPUTATION, CD_TYPIMP_FIELD_LENGTH);
			memcpy(l_strIdLot + DATE_LENGTH + CD_TYPIMP_FIELD_LENGTH, l_strTOP_INT_EXT, CD_TYPEI_FIELD_LENGTH);
			if (strcmp(ENTITY,"LCL") == 0)
			{
				if (strcmp(l_strInputField,"530001001") == 0 || strcmp(l_strInputField,"530001002") == 0  )
				{
					printf("Modification Compte DODGE %s pour ENTITY : LCL en 530001003", l_strInputField);
					strcpy(l_strInputField,"530001003");
					memcpy(o_OutputRecord + tabFieldOfRecord[19].iFieldStartPosOutput, l_strInputField, 9);
				}
				/*else
				{
					printf("Pas de modification Compte DODGE %s pour ENTITY : LCL en 530001003", l_strInputField);
				}*/

			}
		}
		// REF_OPE
		if (strcmp(tabFieldOfRecord[l_iIdx].strFieldName, "MAI_REF_OPE") == 0)
		{
			memcpy(l_strIdLot + DATE_LENGTH + CD_TYPIMP_FIELD_LENGTH + CD_TYPEI_FIELD_LENGTH, l_strInputField + 11, NUM_CRE_IN_CD_REFOPER_LENGTH);
			memcpy(l_strAppliEmet, l_strInputField + 17, 3);
			l_strAppliEmet[3] = '\0';
		}
		l_strIdLot[DATE_LENGTH + CD_TYPIMP_FIELD_LENGTH + CD_TYPEI_FIELD_LENGTH + NUM_CRE_IN_CD_REFOPER_LENGTH] = '\0';
		// printf(" - [Create_Output_Record] - l_strHB_IMPUTATION = %s, l_strTOP_INT_EXT = %s, l_strIdLot = %s.\n", l_strHB_IMPUTATION, l_strTOP_INT_EXT, l_strIdLot);
	
		// Check if Fields to add in Ouput Record
		switch (tabFieldOfRecord[l_iIdx].iFieldType)
		{
			case	ADD_CD_TYPIMP_TYPEI_TVA	: // Add CD_TYPIMP, CD_TYPEI and CD_TVA_APP to Output Record
					l_iIdx++;
					memcpy(l_strInputField, l_strHB_IMPUTATION,  tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
					l_strInputField[tabFieldOfRecord[l_iIdx].iFieldLengthOutput] = '\0';
					Convert_InputField (l_strInputField, &l_iIdx, l_strOutputField);
					memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
					l_iIdx++;
					memcpy(l_strInputField, l_strTOP_INT_EXT, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
					l_strInputField[tabFieldOfRecord[l_iIdx].iFieldLengthOutput] = '\0';
					Convert_InputField (l_strInputField, &l_iIdx, l_strOutputField);
					memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
					l_iIdx++;
					memcpy(l_strInputField, l_strTVA, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
					l_strInputField[tabFieldOfRecord[l_iIdx].iFieldLengthOutput] = '\0';
					Convert_InputField (l_strInputField, &l_iIdx, l_strOutputField);
					memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
					break;
							
			case	UPDATE_MAI_MNT_IMP	: // Add VL_SIGIMP and VL_NBDCIMP to MAI_MNT_IMP in Output File
					break;
					
			case	UPDATE_MAI_MNT_GES	: // Add VL_SIGGEST and VL_NBDGEST to MAI_MNT_GES in Output File
					break;
			
			case	UPDATE_MAI_MNT_NOM	: // Add VL_SIGNOM and VL_NBDCNOM to MAI_MNT_NOM in Output File
					break;
			
			default	: // No Field to be added. The Field of the Input Record is mapped in the Output Record
					break;
		}
		l_iIdx++;
	}
	// printf(" - [Create_Output_Record] - DEV_IMP : %s, Decimal Nr : %s.\n", l_strDEV_IMP, l_strDEV_IMP_DECIMAL_POS);
	// printf(" - [Create_Output_Record] - DEV_GES : %s, Decimal Nr : %s.\n", l_strDEV_GES, l_strDEV_GES_DECIMAL_POS);
	// printf(" - [Create_Output_Record] - DEV_CTP : %s, Decimal Nr : %s.\n", l_strDEV_CTP, l_strDEV_CTP_DECIMAL_POS);
	
	// Build the Header of the Output Record
	memcpy(o_OutputRecord, HEADER_CD_CRE, strlen(HEADER_CD_CRE));
	memcpy(o_OutputRecord + HEADER_CD_CRE_LENGTH, l_strDAT_OPE, 4);
	FindIdLotIdEcriture (l_strAppliEmet, l_strIdLot, l_strLotNum, l_strIdEcriture);
	// printf(" - [Create_Output_Record] - l_strAppliEmet = %s, l_strIdLot = %s, l_strLotNum = %s, l_strIdEcriture = %s\n", l_strAppliEmet, l_strIdLot, l_strLotNum, l_strIdEcriture);
	memcpy(o_OutputRecord + HEADER_CD_CRE_LENGTH + 4, l_strAppliEmet, 3);
	memcpy(o_OutputRecord + HEADER_CD_CRE_LENGTH + 4 + 3, l_strLotNum, 17);
	memcpy(o_OutputRecord + HEADER_CD_CRE_LENGTH + HEADER_ID_LOT_LENGTH + HEADER_ID_COMPOST_LENGTH, l_strIdEcriture, HEADER_ID_ECRITU_LENGTH);
	o_OutputRecord[tabFieldOfRecord[l_iIdx - 1].iFieldStartPosOutput + tabFieldOfRecord[l_iIdx - 1].iFieldLengthOutput] = '\0';
	return EXIT_OK;
}

/* 
 * =============================================================================
 *  Building Format of Output File Record using struct_premai.conf File
 *  In this File, Each Record corresponds to one Field of the Input File Record
 * =============================================================================
 */
int BuildOutputRecordFormat()
{
	FILE *l_InputFile_RecordFormat_Ptr		= NULL;
	char  l_strFullInputFileFormatName[MAX_FULL_FILE_NAME_LENGTH];
	int   l_iFieldNumber					= 0;
	int   l_iRecNumber						= 0;
	int   l_iPreviousInputFieldNumber 		= 0;
	int   l_iStartFieldPositionForInput		= 0;
	int   l_iStartFieldPositionForOutput	= HEADER_LENGTH;

	/* Initializing the table of Fields of Input and Output Files */
	for (l_iFieldNumber = 0; l_iFieldNumber < MAX_FIELD_NUMBER; l_iFieldNumber++)
	{
		memset(tabFieldOfRecord[l_iFieldNumber].strFieldName,   '\0', sizeof(tabFieldOfRecord[l_iFieldNumber].strFieldName));
		memset(tabFieldOfRecord[l_iFieldNumber].strFieldFormat, '\0', sizeof(tabFieldOfRecord[l_iFieldNumber].strFieldFormat));
		tabFieldOfRecord[l_iFieldNumber].iFieldFormat         = unknown;
		tabFieldOfRecord[l_iFieldNumber].iFieldType           = -1;
		tabFieldOfRecord[l_iFieldNumber].iFieldLengthInput    = -1;
		tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = -1;
		tabFieldOfRecord[l_iFieldNumber].iFieldStartPosInput  = -1;
		tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput = -1;
	}
	
	/* Opening struct_premai.conf File */
	strcpy(l_strFullInputFileFormatName, strConfigurationDirectory);
	strcat(l_strFullInputFileFormatName, "/");
	strcat(l_strFullInputFileFormatName, INPUT_FILE_FORMAT_NAME);

	printf("Opening %s File ...\n", l_strFullInputFileFormatName);
	l_InputFile_RecordFormat_Ptr = fopen(l_strFullInputFileFormatName, "r");
	if (l_InputFile_RecordFormat_Ptr == NULL)
	{
		printf("Error %d : '%s' occurs when opening %s File \n", errno, strerror(errno), l_strFullInputFileFormatName);
		return EXIT_ERR;
	}

	/* Reading Records of struct_premai.conf File : Each Record gives the Characteristics of a Field of the Input File Record */
	l_iFieldNumber = -1;
	while (fgets((char*) &Input_File_Record_Format_Struct, MAX_INPUT_FORMAT_REC_LENGTH, l_InputFile_RecordFormat_Ptr) != NULL)
	{
		if (strlen(Input_File_Record_Format_Struct.InputFormat_Record) > 1)
		{
			// Handle only not empty records
			l_iRecNumber++;
			l_iFieldNumber++;
			
			strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldName, strtok(Input_File_Record_Format_Struct.InputFormat_Record, SEPARATOR));
			strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldFormat, strtok(NULL, SEPARATOR));
			tabFieldOfRecord[l_iFieldNumber].iFieldFormat = whichOutputFormat(tabFieldOfRecord[l_iFieldNumber].strFieldFormat);
			// Start Position of the Field in Input and Output Record
			if (l_iRecNumber == 1)
			{
				tabFieldOfRecord[l_iFieldNumber].iFieldStartPosInput  = l_iStartFieldPositionForInput;
				tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput = l_iStartFieldPositionForOutput;
				tabFieldOfRecord[l_iFieldNumber].iFieldLengthInput    = atoi(strtok(NULL, SEPARATOR));
				tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = tabFieldOfRecord[l_iFieldNumber].iFieldLengthInput;
			}
			else
			{
				l_iStartFieldPositionForInput  = l_iStartFieldPositionForInput  + tabFieldOfRecord[l_iPreviousInputFieldNumber].iFieldLengthInput;
				l_iStartFieldPositionForOutput = l_iStartFieldPositionForOutput + tabFieldOfRecord[l_iFieldNumber - 1].iFieldLengthOutput;
				tabFieldOfRecord[l_iFieldNumber].iFieldStartPosInput  = l_iStartFieldPositionForInput;
				tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput = l_iStartFieldPositionForOutput;
				tabFieldOfRecord[l_iFieldNumber].iFieldLengthInput    = atoi(strtok(NULL, SEPARATOR));
				tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = tabFieldOfRecord[l_iFieldNumber].iFieldLengthInput;
			}
			// Input Record Length Calculation
			iInputRecordLength += tabFieldOfRecord[l_iFieldNumber].iFieldLengthInput;
			// Amount Handling
			if (strcmp(tabFieldOfRecord[l_iFieldNumber].strFieldName, "MAI_MNT_IMP") == 0)
			{
				tabFieldOfRecord[l_iFieldNumber].iFieldType           = UPDATE_MAI_MNT_IMP;
				// Add VL_SIGIMP and VL_NBDCIMP to MAI_MNT_IMP in Output File
				strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldFormat, "amount3DEC");
				tabFieldOfRecord[l_iFieldNumber].iFieldFormat         = whichOutputFormat(tabFieldOfRecord[l_iFieldNumber].strFieldFormat);
				tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = SIGN_FIELD_LENGTH + AMOUNT_FIELD_LENGTH + DECIMAL_NR_FIELD_LENGTH;
				l_iPreviousInputFieldNumber = l_iFieldNumber;
			}
			else
			{
				if (strcmp(tabFieldOfRecord[l_iFieldNumber].strFieldName, "MAI_MNT_GES") == 0)
				{
					tabFieldOfRecord[l_iFieldNumber].iFieldType           = UPDATE_MAI_MNT_GES;
					// Add VL_SIGGEST and VL_NBDGEST to MAI_MNT_GES in Output File
					strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldFormat, "amount3DEC");
					tabFieldOfRecord[l_iFieldNumber].iFieldFormat         = whichOutputFormat(tabFieldOfRecord[l_iFieldNumber].strFieldFormat);
					tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = SIGN_FIELD_LENGTH + AMOUNT_FIELD_LENGTH + DECIMAL_NR_FIELD_LENGTH;
					l_iPreviousInputFieldNumber = l_iFieldNumber;
				}
				else
				{
					if (strcmp(tabFieldOfRecord[l_iFieldNumber].strFieldName, "MAI_MNT_NOM") == 0)
					{
						tabFieldOfRecord[l_iFieldNumber].iFieldType           = UPDATE_MAI_MNT_NOM;
						// Add VL_SIGNOM and VL_NBDCNOM to MAI_MNT_NOM in Output File
						strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldFormat, "amount3DEC");
						tabFieldOfRecord[l_iFieldNumber].iFieldFormat         = whichOutputFormat(tabFieldOfRecord[l_iFieldNumber].strFieldFormat);
						tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = SIGN_FIELD_LENGTH + AMOUNT_FIELD_LENGTH + DECIMAL_NR_FIELD_LENGTH;
						l_iPreviousInputFieldNumber = l_iFieldNumber;
					}
					else
					{
						if (strcmp(tabFieldOfRecord[l_iFieldNumber].strFieldName, "MAI_TOP_MAJ") == 0)
						{
							tabFieldOfRecord[l_iFieldNumber].iFieldType           = ADD_CD_TYPIMP_TYPEI_TVA;
							// Add CD_TYPIMP, CD_TYPEI and CD_TVA_APP in Output File
							l_iPreviousInputFieldNumber = l_iFieldNumber;
							l_iFieldNumber++;
							strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldName,   "CD_TYPIMP");
							strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldFormat, "charED");
							tabFieldOfRecord[l_iFieldNumber].iFieldFormat         = whichOutputFormat(tabFieldOfRecord[l_iFieldNumber].strFieldFormat);
							tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = CD_TYPIMP_FIELD_LENGTH;
							tabFieldOfRecord[l_iFieldNumber].iFieldStartPosInput  = CD_TYPIMP_TYPEI_TVA_INPUT_POSIT;
							tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput = tabFieldOfRecord[l_iFieldNumber - 1].iFieldStartPosOutput + tabFieldOfRecord[l_iFieldNumber -1].iFieldLengthOutput;
							l_iFieldNumber++;
							strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldName,   "CD_TYPEI");
							strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldFormat, "charED");
							tabFieldOfRecord[l_iFieldNumber].iFieldFormat         = whichOutputFormat(tabFieldOfRecord[l_iFieldNumber].strFieldFormat);
							tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = CD_TYPEI_FIELD_LENGTH;
							tabFieldOfRecord[l_iFieldNumber].iFieldStartPosInput  = CD_TYPIMP_TYPEI_TVA_INPUT_POSIT;
							tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput = tabFieldOfRecord[l_iFieldNumber - 1].iFieldStartPosOutput + tabFieldOfRecord[l_iFieldNumber -1].iFieldLengthOutput;
							l_iFieldNumber++;
							strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldName,   "CD_TVA_APP");
							strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldFormat, "charED");
							tabFieldOfRecord[l_iFieldNumber].iFieldFormat         = whichOutputFormat(tabFieldOfRecord[l_iFieldNumber].strFieldFormat);
							tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = CD_TVA_APP_FIELD_LENGTH;
							tabFieldOfRecord[l_iFieldNumber].iFieldStartPosInput  = CD_TYPIMP_TYPEI_TVA_INPUT_POSIT;
							tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput = tabFieldOfRecord[l_iFieldNumber - 1].iFieldStartPosOutput + tabFieldOfRecord[l_iFieldNumber -1].iFieldLengthOutput;							
							l_iStartFieldPositionForOutput                        = tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput;
						}
						else
						{
							l_iPreviousInputFieldNumber = l_iFieldNumber;
						}
					}
				}
			}
		}
	}
	
	/* Closing struct_premai.conf File */
	printf("Closing %s File ...\n", l_strFullInputFileFormatName);
	fclose(l_InputFile_RecordFormat_Ptr);
	return EXIT_OK;
}

/* 
 * =============================================================================
 *                              Main  Function
 * =============================================================================
 */
main(int argc, char *argv[])
{
	FILE *InputFile_Ptr     	= NULL;
	FILE *OutputFile_Ptr    	= NULL;
	char *InputFile_Name		= NULL;
	char *OutputFile_Name		= NULL;
	char Accounting_Date[DATE_LENGTH + 1];
	long l_lIdX					= 0;
	long l_lIdY					= 0;
	long l_lIdx					= 0;
	long Record_Number       	= 0;
	long Empty_Record_Number 	= 0;

	/* Start of Program */
	printf("Start Har_Transco_PreMai Program ...\n");
	
	/* Getting Environnement variables */
	if (getenv(CONFIGURATION_DIRECTORY) == NULL)
	{
		printf("Configuration Directory %s is not defined\n", CONFIGURATION_DIRECTORY);
		return EXIT_ERR;
	}
	else
	{
		strConfigurationDirectory = (char*) malloc(MAX_DIRECTORY_LENGTH * sizeof(char));
		strcpy(strConfigurationDirectory, getenv(CONFIGURATION_DIRECTORY));
	}
	
	/* Getting Parameters */
	printf("Getting Parameters ...\n");
	if ((argc < NB_PARAM) || (argc > NB_PARAM + 1))
	{
		printf("Bad Number of Parameters. This Number must be %d or %d instead of %d\n", NB_PARAM - 1, NB_PARAM , argc - 1);
		printf("---                                   U S A G E                                   ---\n");
		printf("   - Parameter 1 : Input File                                          [Mandatory]\n");
		printf("   - Parameter 2 : Accounting Date in YYYYMMDD Format                  [Mandatory]\n");
		printf("   - Parameter 3 : Site : CASA or LCL                                  [Optionnal]\n");
		return EXIT_ERR;
	}
	else
	{
		printf("Input File .................... : %s\n", argv[1]);
		printf("Accounting Date ............... : %s\n", argv[2]);
		
		if (argc == 2+1)
		{
			printf("Site(if NULL => CACIB else CASA or LCL): NULL => CACIB\n");
		}
		if (argc == 3+1)
		{
			printf("Site(if NULL => CACIB else CASA or LCL): %s\n", argv[3]);
		}
		
	}
	/* Checking Parameters */
	printf("Checking Parameters ...\n");
	
	// Input File
	InputFile_Name = (char*) malloc(strlen(argv[1]) * sizeof(char));        
   	strncpy(InputFile_Name, argv[1], strlen(argv[1]));
	InputFile_Name[strlen(argv[1])] = '\0';

	// Accounting Date
	strncpy(Accounting_Date, argv[2], DATE_LENGTH);
	Accounting_Date[DATE_LENGTH] = '\0';
	if (! isValidInputDate(Accounting_Date))
	{
			printf("---                    U S A G E                    ---\n");
			printf("   Enter a Valid Accounting Date in YYYYMMDD Format\n");
			return EXIT_ERR;
	}
	
	// Site
	if (argc == 2+1)
	{
		memcpy(strRCA_CPT_FILE_NAME,REF_RCA_CPT_FILE_NAME, 16);
		printf("le fichier plan de compte est : %s\n",strRCA_CPT_FILE_NAME);
	}
	if (argc == 3+1)
	{
			if (strcmp(argv[3], "CASA") != 0 )
			{
				if (strcmp(argv[3], "LCL") != 0 )
				{
					printf("---                    U S A G E                    ---\n");
					printf("   Enter a Valid Site [NULL] or [CASA] or [LCL]\n");
					return EXIT_ERR;
				}
				else
				{
					strcpy(ENTITY,"LCL");
				 	memcpy(strRCA_CPT_FILE_NAME,REF_RCA_CPT_FILE_NAME, 16);
					printf("le fichier plan de compte est : %s  ENTITY :%s \n",strRCA_CPT_FILE_NAME,ENTITY);
					//memcpy(strRCA_CPT_FILE_NAME,REF_RCA_CPT_CAS_FILE_NAME, 19);
					//printf("le fichier plan de compte est : %s\n",strRCA_CPT_FILE_NAME);
				}
			}
			else
			{			
				memcpy(strRCA_CPT_FILE_NAME,REF_RCA_CPT_CAS_FILE_NAME, 19);
				printf("le fichier plan de compte est : %s\n",strRCA_CPT_FILE_NAME);
			}
	
	}
	
	/* Building Format of Output File Record using struct_premai.conf File */
	if (BuildOutputRecordFormat() == EXIT_ERR)
	{
		return EXIT_ERR;
	}
	/*else
	{
		while (strlen(tabFieldOfRecord[l_lIdx].strFieldName) > 0)
		{
			printf(" - [main] - tabFieldOfRecord - Field Number %03ld : %s \t\t| %s \t\t| %01d \t\t| %03d \t\t| %04d \t\t| %04d \t\t| %04d \t\t| %04d\n", 
						l_lIdx, 
						tabFieldOfRecord[l_lIdx].strFieldName,
						tabFieldOfRecord[l_lIdx].strFieldFormat, 
						tabFieldOfRecord[l_lIdx].iFieldFormat,
						tabFieldOfRecord[l_lIdx].iFieldType,
						tabFieldOfRecord[l_lIdx].iFieldLengthInput,
						tabFieldOfRecord[l_lIdx].iFieldLengthOutput, 
						tabFieldOfRecord[l_lIdx].iFieldStartPosInput, 
						tabFieldOfRecord[l_lIdx].iFieldStartPosOutput);
			l_lIdx++;
		}
	}*/

	/* Getting the REF_RCA_CPT.dat File Format using REF_RCA_CPT.conf File */
	if (BuildRefRcaCptRecordFormat() == EXIT_ERR)
	{
		return EXIT_ERR;
	}
	/*else
	{
		l_lIdx = 0;
		while (strlen(tabFieldOfRefRcaCptRecord[l_lIdx].strFieldName) > 0)
		{
			printf(" - [main] - tabFieldOfRefRcaCptRecord - Field Number %03ld : %s \t\t| %s \t\t| %01d \t\t| %03d \t\t| %04d\n", 
						l_lIdx,
						tabFieldOfRefRcaCptRecord[l_lIdx].strFieldName,
						tabFieldOfRefRcaCptRecord[l_lIdx].strFieldFormat, 
						tabFieldOfRefRcaCptRecord[l_lIdx].iFieldFormat,
						tabFieldOfRefRcaCptRecord[l_lIdx].iFieldLength, 
						tabFieldOfRefRcaCptRecord[l_lIdx].iFieldStartSepPosition);
			l_lIdx++;			
		}
	}
	printf("\n");*/

	/* Build COMPTE_DODGE Table using COMPTE_DODGE in REF_RCA_CPT.dat File */
	if (BuildCompteDodgeTable() == EXIT_ERR)
	{
		return EXIT_ERR;
	}
	/*else
	{
		for (l_lIdX = 0; l_lIdX < HASH_DODGE_ARRAY_SIZE; l_lIdX++)
		{
			for (l_lIdY = 0; l_lIdY < HASH_DODGE_ARRAY_SIZE; l_lIdY++)
			{
				if (COMPTE_DODGEHashArray[l_lIdX].stElt[l_lIdY].COMPTE_DODGEHashKey >= 0)
				{
					printf (" - [main] - COMPTE_DODGEHashArray - Record l_lIdX = %03ld , l_lIdY = %03ld : %lld | \t\t%s | \t\t%s | \t\t%s | \t\t%s | \t\t%s\n",	
								l_lIdX, l_lIdY,
								COMPTE_DODGEHashArray[l_lIdX].stElt[l_lIdY].COMPTE_DODGEHashKey,
								COMPTE_DODGEHashArray[l_lIdX].stElt[l_lIdY].strCOMPTE_DODGE,
								COMPTE_DODGEHashArray[l_lIdX].stElt[l_lIdY].strTOP_BILAN,
								COMPTE_DODGEHashArray[l_lIdX].stElt[l_lIdY].strHB_IMPUTATION,
								COMPTE_DODGEHashArray[l_lIdX].stElt[l_lIdY].strTOP_INT_EXT,
								COMPTE_DODGEHashArray[l_lIdX].stElt[l_lIdY].strTVA);
				}
			}
		}
	}
	printf("\n");*/
	
	/* Getting the REF_CURRENCY.dat File Format using REF_CURRENCY.conf File */
	if (BuildRefCurrencyRecordFormat() == EXIT_ERR)
	{
		return EXIT_ERR;
	}
	/*else
	{
		for (l_lIdX = 0; l_lIdX < MAX_FIELD_NUMBER_CURRENCY; l_lIdX++)
		{
			printf (" - [main] - tabFieldOfRefCurrencyRecord - : strFieldName          = %s\n", tabFieldOfRefCurrencyRecord[l_lIdX].strFieldName);
			printf (" - [main] - tabFieldOfRefCurrencyRecord - : strFieldFormat        = %s\n", tabFieldOfRefCurrencyRecord[l_lIdX].strFieldFormat);
			printf (" - [main] - tabFieldOfRefCurrencyRecord - : iFieldFormat          = %d\n", tabFieldOfRefCurrencyRecord[l_lIdX].iFieldFormat);
			printf (" - [main] - tabFieldOfRefCurrencyRecord - : iFieldLength          = %d\n", tabFieldOfRefCurrencyRecord[l_lIdX].iFieldLength);
			printf (" - [main] - tabFieldOfRefCurrencyRecord - : FieldStartSepPosition = %d\n", tabFieldOfRefCurrencyRecord[l_lIdX].iFieldStartSepPosition);
		}
	}*/
	/* Build CURRENCY Table using CURRENCY_CD in REF_CURRENCY.dat File */
	if (BuildCurrencyTable() == EXIT_ERR)
	{
		return EXIT_ERR;
	}
	/*else
	{	
		for (l_lIdX = 0; l_lIdX < HASH_CURRENCY_ARRAY_SIZE; l_lIdX++)
		{
			for (l_lIdY = 0; l_lIdY < HASH_CURRENCY_ARRAY_SIZE; l_lIdY++)
			{
				if (CURRENCYHashArray[l_lIdX].stElt[l_lIdY].CURRENCYHashKey >= 0)
				{
					printf (" - [main] - CURRENCYHashArray - Record %03ld , %03ld : %lld | \t\t%s | \t\t%s\n",	
								l_lIdX, l_lIdY,
								CURRENCYHashArray[l_lIdX].stElt[l_lIdY].CURRENCYHashKey,
								CURRENCYHashArray[l_lIdX].stElt[l_lIdY].strCURRENCY_CD,
								CURRENCYHashArray[l_lIdX].stElt[l_lIdY].strDECIMAL_POS);
				}
			}
		}
	}
	printf("\n");*/
	
	/* Start Input File Handling */
	printf("Start Handling of %s File\n", InputFile_Name);

	/* Opening Input File */
	printf("Opening Input  File ........... : %s\n", InputFile_Name);
	InputFile_Ptr = fopen(InputFile_Name, "r");
	if (InputFile_Ptr == NULL)
	{
		printf("Error %d : '%s' occurs when opening %s File \n", errno, strerror(errno), InputFile_Name);
		free(InputFile_Name);
		return EXIT_ERR;
	}	
	
	/* Opening Ouput File */
	OutputFile_Name = (char*) malloc((1 + strlen(argv[1]) + strlen(OUTPUT_FILE_EXTENSION)) * sizeof(char));
	strcpy(OutputFile_Name, InputFile_Name);
	strcat(OutputFile_Name, OUTPUT_FILE_EXTENSION);
	printf("Opening Output File ........... : %s\n", OutputFile_Name);
	OutputFile_Ptr = fopen(OutputFile_Name, "w");
	if (OutputFile_Ptr == NULL)
	{
		printf("Error %d : '%s' occurs when opening %s File \n", errno, strerror(errno), OutputFile_Name);
		free(OutputFile_Name);
		return EXIT_ERR;
	}   
	
	/* Initializing LOTHashKeyTable Table */
	InitializeLOTHashKeyTable();
	
	/* Handling Input Data and Creating Output File */
	while (fgets((char*) &Input_Record_Struct, MAX_INPUT_REC_LENGTH, InputFile_Ptr) != NULL)
	{
		if (strlen(Input_Record_Struct.Input_Record) > 1)
		{
			// Handle only not empty Records
			Record_Number++;
			if (strlen(Input_Record_Struct.Input_Record) == iInputRecordLength)
			{
				// Create Output Record
				memset(Output_Record_Struct.Output_Header, ' ', HEADER_LENGTH);
				memset(Output_Record_Struct.Output_Record, ' ', MAX_INPUT_REC_LENGTH + 3 * (SIGN_FIELD_LENGTH + DECIMAL_NR_FIELD_LENGTH) + CD_TYPIMP_FIELD_LENGTH + CD_TYPEI_FIELD_LENGTH + CD_TVA_APP_FIELD_LENGTH);
				if (Create_Output_Record(Input_Record_Struct.Input_Record, (char *) &Output_Record_Struct) == EXIT_ERR)
				{
					printf("Process aborting. Record Nr %ld rejected : %s\n", Record_Number, Input_Record_Struct.Input_Record);
					free(InputFile_Name);
					free(OutputFile_Name);
					return EXIT_ERR;
				}
				else
				{
					fprintf(OutputFile_Ptr, "%s\n", (char *) &Output_Record_Struct);
				}
			}
			else
			{
				// Invalid Input Record Length
				printf("Process aborting. Unexpected Record Length : %ld instead of %ld. Record Nr %ld rejected : %s\n", strlen(Input_Record_Struct.Input_Record), iInputRecordLength, Record_Number, Input_Record_Struct.Input_Record);
				free(InputFile_Name);
				free(OutputFile_Name);
				return EXIT_ERR;				
			}
		}
		else
		{
			Empty_Record_Number++;
		}
	}
	
	/*for (l_lIdX = 0; l_lIdX < HASH_LOT_ARRAY_SIZE; l_lIdX++)
	{
		for (l_lIdY = 0; l_lIdY < HASH_LOT_ARRAY_SIZE; l_lIdY++)
		{
			if (LOTHashArray[l_lIdX].stElt[l_lIdY].iLOT_NUM > 0)
			{
				printf (" - [main] - LOTHashArray - Record l_lIdX = %03ld , l_lIdY = %03ld : %s | \t\t%s | \t\t%s | \t\t%017d | \t\t%06d\n",	
							l_lIdX, l_lIdY,
							LOTHashArray[l_lIdX].stElt[l_lIdY].strAPPLI_EMET_ID_LOT,
							LOTHashArray[l_lIdX].stElt[l_lIdY].strAPPLI_EMET,
							LOTHashArray[l_lIdX].stElt[l_lIdY].strID_LOT,
							LOTHashArray[l_lIdX].stElt[l_lIdY].iLOT_NUM,
							LOTHashArray[l_lIdX].stElt[l_lIdY].iID_ECRITU);
			}
		}
	}
	printf("\n");*/
	
	printf("Total Number of Records Read .. : %ld\n", Record_Number + Empty_Record_Number);
	printf("Total Number of Handled Records : %ld\n", Record_Number);

	/* End Input File Handling */
	printf("End   Handling of %s File\n", InputFile_Name);
	
	/* Closing Files */
	printf("Closing %s and %s Files ...\n", OutputFile_Name, InputFile_Name);
	fclose(OutputFile_Ptr);
	fclose(InputFile_Ptr);

	/* Free memory allocation */
	free(InputFile_Name);
	free(OutputFile_Name);
	
	/* End of Program */
	printf("End   Har_Transco_PreMai Program ...\n");
}
