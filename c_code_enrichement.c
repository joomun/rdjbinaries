/* 
 *=======================================================================================
 * NAME        : Har_Transco_PESTD.c
 * DESCRIPTION : Enrichment and Formatting of Pre-ESTD Data File for RDJ Handling
 * AUTHOR      : Z. BERRAH
 * CREATION    : 2011/11/25
 *=======================================================================================
 *                                  U S A G E
 *
 *         Har_Transco_PESTD <Parameter 1> <Parameter 2>
 *       - Parameter 1 : Input File                         [Mandatory]
 *       - Parameter 2 : Accounting Date in YYYYMMDD Format [Mandatory]
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
#define TIME_LENGTH						6
#define DEFAULT_INPUT_TIME				"000000"
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
#define HASH_ARRAY_SIZE		  			1500
#define HASH_CURRENCY_ARRAY_SIZE		50
#define HASH_KEY_FOUND			   		1
#define HASH_KEY_NOT_FOUND				0
#define MAX_HASH_KEY_LENGTH				18	// Hash Key is a 'long long' variable. Its Maximal Value is : 9 223 372 036 854 775 807
											// The string "zzzzzzzzzzzzzzzzzz" gives the highest value of Hash Key : 9 999 999 999 999 999 990

/* File Handling */
#define MAX_INPUT_REC_LENGTH			3500
#define MAX_INPUT_FORMAT_REC_LENGTH		100
#define HEADER_LENGTH					155
#define HEADER_MVT_LABEL				"INV_MARCHE"
#define HEADER_MVT_LABEL_LENGTH			20
#define OUTPUT_FILE_EXTENSION			".out"
#define CONFIGURATION_DIRECTORY			"RDJ_DAT"
#define INPUT_FILE_FORMAT_NAME			"struct_pestd.conf"
#define INPUT_FILE_SEPARATOR			"|"
#define REF_TIERS_FORMAT_FILE_NAME		"REF_TIERS.conf"
#define REF_TIERS_FILE_NAME				"REF_TIERS.dat"
#define REF_TIERS_FIELD_NAME_LENGTH		50
#define REF_CURRENCY_FORMAT_FILE_NAME	"REF_CURRENCY.conf"
#define REF_CURRENCY_FILE_NAME			"REF_CURRENCY.dat"
#define MAX_DIRECTORY_LENGTH			500
#define MAX_FULL_FILE_NAME_LENGTH		500

/* TIERS RICOS Informations */
#define TIERS_RICOS_LENGTH				50
#define TIERS_RICOS_FIELD_LENGTH		12
#define TIERS_RICOS_FIELD_NUMBER		11
#define REF_TIERS_SEPARATOR				";"
#define MAX_SEPARATOR_IN_REF_TIERS		5
#define REF_TIERS_RECORD_LENGTH			300
#define ADD_RICOS_SC_CPY_USING_SIAM		10
#define ADD_RICOS_SC_CPY_USING_RTS		100
#define ADD_RICOS_SC_USING_RTS			200
#define RICOS_SC_CPY_SIAM_INPUT_POSIT	-10
#define RICOS_SC_CPY_RTS_INPUT_POSIT	-100
#define RICOS_SC_RTS_INPUT_POSIT		-200

/* Currency Informations */
#define MAX_CURRENCY_FORMAT_REC_LENGTH	100
#define REF_CURRENCY_FIELD_NAME_LENGTH	50
#define REF_CURRENCY_RECORD_LENGTH		50
#define REF_CURRENCY_SEPARATOR			";"
#define MAX_SEPARATOR_IN_REF_CURRENCY	1	// Number of separators in REF_CURRENCY.dat File
#define MAX_FIELD_NUMBER_CURRENCY		2	// Number of records in REF_CURRENCY.conf File
#define CURRENCY_CD_LENGTH				3
#define EMPTY_CURRENCY					"   "

char strRefTiersFileFormat[REF_TIERS_RECORD_LENGTH];
char RefTiersRicos_Record[REF_TIERS_RECORD_LENGTH];
char RefCurrency_Record[REF_CURRENCY_RECORD_LENGTH];
char *strConfigurationDirectory			= NULL;
	
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

/* Data of REF_CURRENCY.dat File */ 
struct
{
	char strCURRENCY_CD[CURRENCY_CD_LENGTH + 1];
	char strDECIMAL_POS[1 + 1];
}	RefCurrencyFile_Struct;

/* Data of REF_TIERS.dat File */ 
struct
{
	char strSIAM[TIERS_RICOS_LENGTH];
	char strRICOS_SC_ID[TIERS_RICOS_LENGTH];
	char strRICOS_CPY_ID[TIERS_RICOS_LENGTH];
	char strRTS_SC_ID[TIERS_RICOS_LENGTH];
	char strSC_INTITULE_USUEL[TIERS_RICOS_LENGTH];
	char strSIA_CIT_TYPE[TIERS_RICOS_LENGTH];
}	RefTiersFile_Struct;

/* SIAMHashArray Table */
typedef struct stSIAMHashElt
{
	long long	SIAMHashKey;
	char 		strSIAM[TIERS_RICOS_LENGTH];
	char 		strRICOS_SC_ID[TIERS_RICOS_LENGTH];
	char 		strRICOS_CPY_ID[TIERS_RICOS_LENGTH];
}	SIAMHashElt;

struct
{
	SIAMHashElt		stElt[HASH_ARRAY_SIZE];
}	SIAMHashArray[HASH_ARRAY_SIZE];

/* RTSHashArray Table */
typedef struct stRTSHashElt
{
	long long	RTSHashKey;
	char 		strRTS_SC_ID[TIERS_RICOS_LENGTH];
	char 		strRICOS_SC_ID[TIERS_RICOS_LENGTH];
	char 		strRICOS_CPY_ID[TIERS_RICOS_LENGTH];
}	RTSHashElt;

struct
{
	RTSHashElt		stElt[HASH_ARRAY_SIZE];
}	RTSHashArray[HASH_ARRAY_SIZE];

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

/* Table of the different Fields of the REF_CURRENCY.dat File */
struct
{
	char			strFieldName[FIELD_NAME_LENGTH];
	char			strFieldFormat[FIELD_FORMAT_LENGTH];
	enumFieldFormat	iFieldFormat;
	int				iFieldLength;
	int				iFieldStartSepPosition;
}	tabFieldOfRefCurrencyRecord[MAX_FIELD_NUMBER_CURRENCY + 1];

/* Record of struct_pestd.conf File */
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
	char Output_Header[HEADER_LENGTH];
	char Output_Record[MAX_INPUT_REC_LENGTH + TIERS_RICOS_FIELD_NUMBER * TIERS_RICOS_FIELD_LENGTH];
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
 *                        Is the Input Time Valid ?
 * =============================================================================
*/
int isValidInputTime (const char *i_strTime)
{
	char  l_strHour[2+1];
	char  l_strMinute[2+1];
	char  l_strSecond[2+1];
	int   l_iIdx     = 0;
	int   l_iHour    = 0;
	int   l_iMinute  = 0;
	int   l_iSecond  = 0;
	
	/* Input Time must be Numeric */
	for(l_iIdx=0; l_iIdx < TIME_LENGTH; l_iIdx++)
	{
		if (! isdigit(i_strTime[l_iIdx]))
		{
			// printf("The Input Time %s is Invalid !!!\n", i_strTime);
			return FALSE;
		}
	}
	/* Numeric Input Time must be Valid */
	memcpy(l_strHour,   i_strTime, 2);
	l_strHour[2]  = '\0';
	memcpy(l_strMinute, i_strTime + 2, 2);
	l_strMinute[2] = '\0';
	memcpy(l_strSecond, i_strTime + 4, 2);
	l_strSecond[2]   = '\0';
	l_iHour   = atoi(l_strHour);
	l_iMinute = atoi(l_strMinute);
	l_iSecond = atoi(l_strSecond);

    if ( l_iHour < 0 || l_iHour > 23 )
	{
        return FALSE;
	}
	else
	{
		if ( l_iMinute < 0 || l_iMinute > 59 )
		{
			return FALSE;
		}
		else
		{
			if ( l_iSecond < 0 || l_iSecond > 59)
			{
				return FALSE;
			}
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
 *              Initialize SIAM Hash Key Table
 * =============================================================================
*/
void InitializeSIAMHashKeyTable	()
{
	long	l_lIdX;
	long	l_lIdY;
	
	for (l_lIdX = 0; l_lIdX < HASH_ARRAY_SIZE; l_lIdX++)
	{
		for (l_lIdY = 0; l_lIdY < HASH_ARRAY_SIZE; l_lIdY++)
		{
			SIAMHashArray[l_lIdX].stElt[l_lIdY].SIAMHashKey = -1;
			memset(SIAMHashArray[l_lIdX].stElt[l_lIdY].strSIAM,        '\0', sizeof(SIAMHashArray[l_lIdX].stElt[l_lIdY].strSIAM));
			memset(SIAMHashArray[l_lIdX].stElt[l_lIdY].strRICOS_SC_ID, '\0', sizeof(SIAMHashArray[l_lIdX].stElt[l_lIdY].strRICOS_SC_ID));
			memset(SIAMHashArray[l_lIdX].stElt[l_lIdY].strRICOS_CPY_ID,'\0', sizeof(SIAMHashArray[l_lIdX].stElt[l_lIdY].strRICOS_CPY_ID));
		}
	}
}

/*
 * =============================================================================
 *              Initialize RTS Hash Key Table
 * =============================================================================
*/
void InitializeRTSHashKeyTable	()
{
	long	l_lIdX;
	long	l_lIdY;
	
	for (l_lIdX = 0; l_lIdX < HASH_ARRAY_SIZE; l_lIdX++)
	{
		for (l_lIdY = 0; l_lIdY < HASH_ARRAY_SIZE; l_lIdY++)
		{
			RTSHashArray[l_lIdX].stElt[l_lIdY].RTSHashKey = -1;
			memset(RTSHashArray[l_lIdX].stElt[l_lIdY].strRTS_SC_ID,   '\0', sizeof(RTSHashArray[l_lIdX].stElt[l_lIdY].strRTS_SC_ID));
			memset(RTSHashArray[l_lIdX].stElt[l_lIdY].strRICOS_SC_ID, '\0', sizeof(RTSHashArray[l_lIdX].stElt[l_lIdY].strRICOS_SC_ID));
			memset(RTSHashArray[l_lIdX].stElt[l_lIdY].strRICOS_CPY_ID,'\0', sizeof(RTSHashArray[l_lIdX].stElt[l_lIdY].strRICOS_CPY_ID));
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
 *               Build Hash Key used to access SIAM, RTS and  CURRENCY Tables
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
 *                   Add SIAM HaskKey in SIAMHashArray Table
 * =============================================================================
*/
void AddElementInSIAMHashKeyTable ()
{
	long		l_lPositYHashKey	= 0;
	long		l_lIdX				= 0;
	long long   l_llHashKey			= 0;

	l_lPositYHashKey = BuildHashKey (RefTiersFile_Struct.strSIAM, HASH_ARRAY_SIZE, &l_llHashKey);

	for (l_lIdX = 0; l_lIdX < HASH_ARRAY_SIZE; l_lIdX++)
	{
		if (SIAMHashArray[l_lIdX].stElt[l_lPositYHashKey].SIAMHashKey != -1)
		{
			// Free Position not found. Check Duplicate Key
			if (strcmp(SIAMHashArray[l_lIdX].stElt[l_lPositYHashKey].strSIAM, RefTiersFile_Struct.strSIAM) == 0)
			{
				// printf(" - AddElementInSIAMHashKeyTable - Duplicate Key : Key %s already added in Hash Table\n", RefTiersFile_Struct.strSIAM);
				l_lIdX = HASH_ARRAY_SIZE;
			}
		}
		else
		{
			// Free Position found. Add SIAM HaskKey in SIAMHashArray Table
			SIAMHashArray[l_lIdX].stElt[l_lPositYHashKey].SIAMHashKey = l_llHashKey;
			strcpy(SIAMHashArray[l_lIdX].stElt[l_lPositYHashKey].strSIAM, RefTiersFile_Struct.strSIAM);
			strcpy(SIAMHashArray[l_lIdX].stElt[l_lPositYHashKey].strRICOS_SC_ID, RefTiersFile_Struct.strRICOS_SC_ID);
			strcpy(SIAMHashArray[l_lIdX].stElt[l_lPositYHashKey].strRICOS_CPY_ID, RefTiersFile_Struct.strRICOS_CPY_ID);
			l_lIdX = HASH_ARRAY_SIZE;
		}
	}
}

/*
 * =============================================================================
 *                   Add RTS_SC_ID HaskKey in RTSHashArray Table
 * =============================================================================
*/
void AddElementInRTSHashKeyTable ()
{
	long		l_lPositYHashKey	= 0;
	long		l_lIdX				= 0;
	long long   l_llHashKey			= 0;

	l_lPositYHashKey = BuildHashKey (RefTiersFile_Struct.strRTS_SC_ID, HASH_ARRAY_SIZE, &l_llHashKey);

	for (l_lIdX = 0; l_lIdX < HASH_ARRAY_SIZE; l_lIdX++)
	{
		if (RTSHashArray[l_lIdX].stElt[l_lPositYHashKey].RTSHashKey != -1)
		{
			// Free Position not found. Check Duplicate Key
			if (strcmp(RTSHashArray[l_lIdX].stElt[l_lPositYHashKey].strRTS_SC_ID, RefTiersFile_Struct.strRTS_SC_ID) == 0)
			{
				// printf(" - AddElementInRTSHashKeyTable - Duplicate Key : Key %s already added in Hash Table\n", RefTiersFile_Struct.strRTS_SC_ID);
				l_lIdX = HASH_ARRAY_SIZE;
			}
		}
		else
		{
			// Free Position found. Add RTS_SC_ID HaskKey in RTSHashArray Table
			RTSHashArray[l_lIdX].stElt[l_lPositYHashKey].RTSHashKey = l_llHashKey;
			strcpy(RTSHashArray[l_lIdX].stElt[l_lPositYHashKey].strRTS_SC_ID, RefTiersFile_Struct.strRTS_SC_ID);
			strcpy(RTSHashArray[l_lIdX].stElt[l_lPositYHashKey].strRICOS_SC_ID, RefTiersFile_Struct.strRICOS_SC_ID);
			strcpy(RTSHashArray[l_lIdX].stElt[l_lPositYHashKey].strRICOS_CPY_ID, RefTiersFile_Struct.strRICOS_CPY_ID);
			l_lIdX = HASH_ARRAY_SIZE;
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
 *                 Find an Element in SIAMHashArray Table
 * =============================================================================
*/
long FindElementInSIAMHashArrayTable (const char i_strKey[], long *o_lPositXHashKey, long *o_lPositYHashKey)
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
	strcat(l_strKey,";");

	// printf(" - [FindElementInSIAMHashArrayTable] - i_strKey = %s, l_strKey = %s.\n", i_strKey, l_strKey);

	if (strlen(l_strKey) > 0)
	{
		l_lPositYHashKey = BuildHashKey (l_strKey, HASH_ARRAY_SIZE, &l_llHashKey);

		// Find l_lIdX Position in SIAMHashArray Table
		for (l_lIdX = 0; l_lIdX < HASH_ARRAY_SIZE; l_lIdX++)
		{
			if (strlen(SIAMHashArray[l_lIdX].stElt[l_lPositYHashKey].strSIAM) > 0)
			{		
				// printf(" - [FindElementInSIAMHashArrayTable] - SIAMHashArray[%03ld].stElt[%03ld].strSIAM = %s.\n", l_lIdX, l_lPositYHashKey, SIAMHashArray[l_lIdX].stElt[l_lPositYHashKey].strSIAM);
				if (memcmp(SIAMHashArray[l_lIdX].stElt[l_lPositYHashKey].strSIAM, l_strKey, strlen(l_strKey)) == 0)
				{
					// Key found in SIAMHashArray Table
					*o_lPositXHashKey = l_lIdX;
					*o_lPositYHashKey = l_lPositYHashKey;
					return HASH_KEY_FOUND;
				}
			}
			else
			{
				l_lIdX = HASH_ARRAY_SIZE;
			}				
		}
		// Key NOT found in SIAMHashArray Table
		printf(" - [FindElementInSIAMHashArrayTable] - SIAM Key %s NOT FOUND in SIAMHashArray Table\n", l_strKey);
		*o_lPositXHashKey = -1;
		*o_lPositYHashKey = -1;
		return HASH_KEY_NOT_FOUND;
	}
	else
	{
		// Key is only filled by spaces
		// printf(" - [FindElementInSIAMHashArrayTable] - SIAM Key is Empty\n");
		*o_lPositXHashKey = -1;
		*o_lPositYHashKey = -1;
		return HASH_KEY_NOT_FOUND;
	}
}

/*
 * =============================================================================
 *                 Find an Element in RTSHashArray Table
 * =============================================================================
*/
long FindElementInRTSHashArrayTable (const char i_strKey[], long *o_lPositXHashKey, long *o_lPositYHashKey)
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

	// printf(" - [FindElementInRTSHashArrayTable] - i_strKey = %s, l_strKey = %s.\n", i_strKey, l_strKey);
	
	if (strlen(l_strKey) > 0)
	{
		l_lPositYHashKey = BuildHashKey (l_strKey, HASH_ARRAY_SIZE, &l_llHashKey);

		// Find l_lIdX Position in RTSHashArray Table
		for (l_lIdX = 0; l_lIdX < HASH_ARRAY_SIZE; l_lIdX++)
		{
			if (strlen(RTSHashArray[l_lIdX].stElt[l_lPositYHashKey].strRTS_SC_ID) > 0)
			{		
				// printf(" - [FindElementInRTSHashArrayTable] - RTSHashArray[%03ld].stElt[%03ld].strRTS_SC_ID = %s.\n", l_lIdX, l_lPositYHashKey, RTSHashArray[l_lIdX].stElt[l_lPositYHashKey].strRTS_SC_ID);
				if (memcmp(RTSHashArray[l_lIdX].stElt[l_lPositYHashKey].strRTS_SC_ID, l_strKey, strlen(l_strKey)) == 0)
				{
					// Key found in RTSHashArray Table
					*o_lPositXHashKey = l_lIdX;
					*o_lPositYHashKey = l_lPositYHashKey;
					return HASH_KEY_FOUND;
				}
			}
			else
			{
				l_lIdX = HASH_ARRAY_SIZE;
			}			
		}
		// Key NOT found in RTSHashArray Table
		printf(" - [FindElementInRTSHashArrayTable] - RTS Key %s NOT FOUND in RTSHashArray Table\n", l_strKey);
		*o_lPositXHashKey = -1;
		*o_lPositYHashKey = -1;
		return HASH_KEY_NOT_FOUND;
	}
	else
	{
		// Key is only filled by spaces
		// printf(" - [FindElementInRTSHashArrayTable] - RTS Key is Empty\n");
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
 *  Building Format of REF_TIERS.dat File using REF_TIERS.conf File
 * =============================================================================
 */
int BuildRefTiersRecordFormat()
{
	FILE *l_RefTiersFile_RecordFormat_Ptr	= NULL;
	char  l_strFullRefTiersFileFormatName[MAX_FULL_FILE_NAME_LENGTH];
	char  l_strRefTiersField[REF_TIERS_FIELD_NAME_LENGTH];
	int   l_iTiersFieldNumber				= -1;
	int   l_iTiersFieldLength				= 0;

	/* Opening REF_TIERS.conf File : structure of REF_TIERS.dat File */
	strcpy(l_strFullRefTiersFileFormatName, strConfigurationDirectory);
	strcat(l_strFullRefTiersFileFormatName, "/");
	strcat(l_strFullRefTiersFileFormatName, REF_TIERS_FORMAT_FILE_NAME);
	
	printf("Opening %s File ...\n", l_strFullRefTiersFileFormatName);
	l_RefTiersFile_RecordFormat_Ptr = fopen(l_strFullRefTiersFileFormatName, "r");
	if (l_RefTiersFile_RecordFormat_Ptr == NULL)
	{
		printf("Error %d : '%s' occurs when opening %s File \n", errno, strerror(errno), l_strFullRefTiersFileFormatName);
		return EXIT_ERR;
	}

	/* Reading Records of REF_TIERS.conf File : Each Record gives the Name of a Field of REF_TIERS.dat File Record */
	while (fgets(l_strRefTiersField, REF_TIERS_FIELD_NAME_LENGTH, l_RefTiersFile_RecordFormat_Ptr) != NULL)
	{
		if (strlen(l_strRefTiersField) > 1)
		{
			// Handle only not empty records
			l_iTiersFieldNumber++;
			
			if (memcmp(l_strRefTiersField, "SIA_CIT_TYPE", 12) == 0)
			{
				// Last Field of REF_TIERS.conf File
				memcpy(strRefTiersFileFormat + l_iTiersFieldLength, l_strRefTiersField, strlen(l_strRefTiersField) - 1);
				l_iTiersFieldLength = l_iTiersFieldLength + strlen(l_strRefTiersField) - 1;
				strRefTiersFileFormat[l_iTiersFieldLength] = '\0';
			}
			else
			{
				memcpy(strRefTiersFileFormat + l_iTiersFieldLength, l_strRefTiersField, strlen(l_strRefTiersField) - 1);
				l_iTiersFieldLength = l_iTiersFieldLength + strlen(l_strRefTiersField) - 1;
				memcpy(strRefTiersFileFormat + l_iTiersFieldLength, REF_TIERS_SEPARATOR, 1);
				l_iTiersFieldLength = l_iTiersFieldLength + 1;
			}
		}
	}
	// strRefTiersFileFormat must be equal to : "SIAM;RICOS_SC_ID;RICOS_CPY_ID;RTS_SC_ID;SC_INTITULE_USUEL;SIA_CIT_TYPE"
	if (memcmp(strRefTiersFileFormat, "SIAM;RICOS_SC_ID;RICOS_CPY_ID;RTS_SC_ID;SC_INTITULE_USUEL;SIA_CIT_TYPE", 70) != 0)
	{
		printf("Unexpected Format for %s File : %s\n", l_strFullRefTiersFileFormatName, strRefTiersFileFormat);
		return EXIT_ERR;
	}
	/* Closing REF_TIERS.conf File */
	printf("Closing %s File ...\n", l_strFullRefTiersFileFormatName);
	fclose(l_RefTiersFile_RecordFormat_Ptr);
	return EXIT_OK;
}

/* 
 * =============================================================================
 *  Building TIERS RICOS Tables using SIAM and RTS TIERS in REF_TIERS.dat File
 * =============================================================================
 */
int BuildTiersRicosTables()
{
	FILE *l_RefTiersRicosFile_Ptr	= NULL;
	char  l_strFullRefTiersFileName[MAX_FULL_FILE_NAME_LENGTH];
	int  l_iIdx						= 0;
	int  l_iSepPositInRecord		= 0;
	int  l_iNbSepInRecord			= 0;
	int  l_iRecNumber				= 0;
	long SIAMHashKey				= 0;
	long RTSHashKey					= 0;
	long long l_llHashKeySIAM		= 0;
	long long l_llHashKeyRTS		= 0;

	/* Initializing SIAM and RTS Hash Key Tables */
	InitializeSIAMHashKeyTable ();
	InitializeRTSHashKeyTable ();
	
	/* Opening REF_TIERS.dat File */
	strcpy(l_strFullRefTiersFileName, strConfigurationDirectory);
	strcat(l_strFullRefTiersFileName, "/");
	strcat(l_strFullRefTiersFileName, REF_TIERS_FILE_NAME);

	printf("Opening %s File ...\n", l_strFullRefTiersFileName);
	l_RefTiersRicosFile_Ptr = fopen(l_strFullRefTiersFileName, "r");
	if (l_RefTiersRicosFile_Ptr == NULL)
	{
		printf("Error %d : '%s' occurs when opening %s File \n", errno, strerror(errno), l_strFullRefTiersFileName);
		return EXIT_ERR;
	}

	// Reading Records of REF_TIERS.dat File : Each Record gives the "TIERS RICOS contrepartie"
	// and "TIERS RICOS sous-contrepartie" Format corresponding to ACRO_SIAM / RTS Format
	// Example of REF_TIERS.dat record : CALYHKG;SC0000002325;2091;6847;CALYON;SIA
	l_iRecNumber = -1;
	while (fgets(RefTiersRicos_Record, REF_TIERS_RECORD_LENGTH, l_RefTiersRicosFile_Ptr) != NULL)
	{
		if (strlen(RefTiersRicos_Record) > 1)
		{
			// Handle only not empty records
			memset(RefTiersFile_Struct.strSIAM,              '\0', sizeof(RefTiersFile_Struct.strSIAM));
			memset(RefTiersFile_Struct.strRICOS_SC_ID,       '\0', sizeof(RefTiersFile_Struct.strRICOS_SC_ID));
			memset(RefTiersFile_Struct.strRICOS_CPY_ID,      '\0', sizeof(RefTiersFile_Struct.strRICOS_CPY_ID));
			memset(RefTiersFile_Struct.strRTS_SC_ID,         '\0', sizeof(RefTiersFile_Struct.strRTS_SC_ID));
			memset(RefTiersFile_Struct.strSC_INTITULE_USUEL, '\0', sizeof(RefTiersFile_Struct.strSC_INTITULE_USUEL));
			memset(RefTiersFile_Struct.strSIA_CIT_TYPE,      '\0', sizeof(RefTiersFile_Struct.strSIA_CIT_TYPE));
			
			
			l_iNbSepInRecord=0;
			l_iSepPositInRecord=-1;
			l_iRecNumber++;
			for(l_iIdx=0; l_iIdx < strlen(RefTiersRicos_Record); l_iIdx++)
			{
				if (memcmp(RefTiersRicos_Record + l_iIdx, REF_TIERS_SEPARATOR, 1) == 0)
				{
					l_iNbSepInRecord++;
					switch (l_iNbSepInRecord)
					{
						case 1	:	memcpy(RefTiersFile_Struct.strSIAM, RefTiersRicos_Record + l_iSepPositInRecord + 1, l_iIdx - l_iSepPositInRecord - 1);
									RefTiersFile_Struct.strSIAM[l_iIdx - l_iSepPositInRecord] = '\0';
									break;
						case 2	:	memcpy(RefTiersFile_Struct.strRICOS_SC_ID, RefTiersRicos_Record + l_iSepPositInRecord + 1, l_iIdx - l_iSepPositInRecord - 1);
									RefTiersFile_Struct.strRICOS_SC_ID[l_iIdx - l_iSepPositInRecord - 1] = '\0';
									break;
						case 3	:	memcpy(RefTiersFile_Struct.strRICOS_CPY_ID, RefTiersRicos_Record + l_iSepPositInRecord + 1 , l_iIdx - l_iSepPositInRecord - 1);
									RefTiersFile_Struct.strRICOS_CPY_ID[l_iIdx - l_iSepPositInRecord - 1] = '\0';
									break;
						case 4	:	memcpy(RefTiersFile_Struct.strRTS_SC_ID, RefTiersRicos_Record + l_iSepPositInRecord + 1, l_iIdx - l_iSepPositInRecord - 1);
									RefTiersFile_Struct.strRTS_SC_ID[l_iIdx - l_iSepPositInRecord - 1] = '\0';
									break;
						case 5	:	memcpy(RefTiersFile_Struct.strSC_INTITULE_USUEL, RefTiersRicos_Record + l_iSepPositInRecord + 1, l_iIdx - l_iSepPositInRecord - 1);
									RefTiersFile_Struct.strSC_INTITULE_USUEL[l_iIdx - l_iSepPositInRecord - 1] = '\0';

									memcpy(RefTiersFile_Struct.strSIA_CIT_TYPE, RefTiersRicos_Record + l_iIdx + 1, strlen(RefTiersRicos_Record) - l_iIdx - 2);
									RefTiersFile_Struct.strSIA_CIT_TYPE[strlen(RefTiersRicos_Record) - l_iIdx - 2] = '\0';
									break;									
					}
					l_iSepPositInRecord = l_iIdx;
				}
			}
			if (l_iNbSepInRecord != MAX_SEPARATOR_IN_REF_TIERS)
			{
				RefTiersRicos_Record[strlen(RefTiersRicos_Record) - 1] = '\0';
				printf(" - [BuildTiersRicosTables] - Number of separator %d instead of %d in %s File - Record %06d rejected : \"%s\"\n", l_iNbSepInRecord, MAX_SEPARATOR_IN_REF_TIERS, l_strFullRefTiersFileName, l_iRecNumber, RefTiersRicos_Record);
			}
			else
			{
				if ((strlen(RefTiersFile_Struct.strSIAM) == 0) && (strlen(RefTiersFile_Struct.strRTS_SC_ID) == 0))
				{
					// Record rejected
					RefTiersRicos_Record[strlen(RefTiersRicos_Record) - 1] = '\0';
					printf(" - [BuildTiersRicosTables] - SIAM and RTS Fields NOT FOUND in %s File - Record %06d rejected : \"%s\"\n", l_strFullRefTiersFileName, l_iRecNumber, RefTiersRicos_Record);
				}
				else
				{
					// Record taken into account
					// printf(" - [BuildTiersRicosTables] - %s Record %03d : %s. \t\t| %s. \t\t| %s. \t\t| %s. \t\t| %s. \t\t| %s.\n", REF_TIERS_FILE_NAME, l_iRecNumber, RefTiersFile_Struct.strSIAM, RefTiersFile_Struct.strRICOS_SC_ID, RefTiersFile_Struct.strRICOS_CPY_ID, RefTiersFile_Struct.strRTS_SC_ID, RefTiersFile_Struct.strSC_INTITULE_USUEL, RefTiersFile_Struct.strSIA_CIT_TYPE);
					if (strlen(RefTiersFile_Struct.strSIAM) > 0)
					{
						AddElementInSIAMHashKeyTable ();
					}
					if (strlen(RefTiersFile_Struct.strRTS_SC_ID) > 0)
					{
						AddElementInRTSHashKeyTable ();
					}
				}
			}
		}
	}
	/* Closing REF_TIERS.dat File */
	printf("Closing %s File ...\n", l_strFullRefTiersFileName);
	fclose(l_RefTiersRicosFile_Ptr);
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
								// printf(" - [Convert_InputField] - i_strInputField = %s, l_strInputField = %s, l_iSign = %d.\n", i_strInputField, l_strInputField, l_iSign);
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

		case amount3DEC	:	// NOT USED in this Program because the Amounts in Input File have not decimals
							// Amount with the following Format : Sign, Amount and Number of decimals
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
	int  l_iIdx   			= 0;
	int  l_iSign  			= 0;
	long l_lPositXHashKey	= -1;
	long l_lPositYHashKey	= -1;
	char l_strOutputAmount[20 + 1];
	char l_strInputField[MAX_FIELD_LENGTH];
	char l_strOutputField[MAX_FIELD_LENGTH];
	char l_strDEVISE_ISO[3 + 1];
	char l_strDEVISE_ISO_DECIMAL_POS[1 + 1];
	char l_strZ_MNT_ESTD_DEVISE[20 + 1];
	char l_strEMISS_CRS_Date[DATE_LENGTH + 1];
	char l_strEMISS_CRS_Time[TIME_LENGTH + 1];

	// Initialze l_strInputField and l_strOutputField
	memset(l_strInputField,  ' ', MAX_FIELD_LENGTH);
	memset(l_strOutputField, ' ', MAX_FIELD_LENGTH);
	
	while (strlen(tabFieldOfRecord[l_iIdx].strFieldName) > 0)
	{
		// Check position of separator in Input Record
		/*if (memcmp(i_InputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosInput + tabFieldOfRecord[l_iIdx].iFieldLengthInput, INPUT_FILE_SEPARATOR, 1) != 0)
		{
			// Unexpected position of separator in Input Record
			printf("The Input Record has an Invalid Format. After Field Nr %ld named \"%s\", the separator is expected at position : %ld.\n", l_iIdx + 1, tabFieldOfRecord[l_iIdx].strFieldName, tabFieldOfRecord[l_iIdx].iFieldStartPosInput + tabFieldOfRecord[l_iIdx].iFieldLengthInput + 1);
			return EXIT_ERR;
		}*/
		// Correct position of separator in Input Record
		memcpy(l_strInputField, i_InputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosInput, tabFieldOfRecord[l_iIdx].iFieldLengthInput);
		l_strInputField[tabFieldOfRecord[l_iIdx].iFieldLengthInput] = '\0';
		Convert_InputField (l_strInputField, &l_iIdx, l_strOutputField);
		memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);

		// Emission Date : EMISS_CRS
		if (strcmp(tabFieldOfRecord[l_iIdx].strFieldName, "EMISS_CRS") == 0)
		{
			// For unknown reasons, we do not check if the Emission Date is valid in EMISS_CRS
			// Valid Emission Time in EMISS_CRS ?			
			memcpy(l_strEMISS_CRS_Time, l_strOutputField + DATE_LENGTH, TIME_LENGTH);
			l_strEMISS_CRS_Time[TIME_LENGTH] = '\0';
			if (! isValidInputTime (l_strEMISS_CRS_Time))
			{
				// Invalid Emission Time in EMISS_CRS
				memcpy(l_strEMISS_CRS_Time, DEFAULT_INPUT_TIME, TIME_LENGTH);
				l_strEMISS_CRS_Time[TIME_LENGTH] = '\0';
			}
			memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput + DATE_LENGTH, l_strEMISS_CRS_Time, TIME_LENGTH);
		}
		// Currency : CODE_DEVISE_ISO
		if (strcmp(tabFieldOfRecord[l_iIdx].strFieldName, "CODE_DEVISE_ISO") == 0)
		{
			strcpy(l_strDEVISE_ISO, l_strInputField);
			if (strcmp(l_strDEVISE_ISO, EMPTY_CURRENCY) == 0)
			{
				// Currency Field Empty
				l_strDEVISE_ISO_DECIMAL_POS[0] = '3';
				l_strDEVISE_ISO_DECIMAL_POS[1] = '\0';
				printf("Currency Field Empty. Default Decimal Number = %s\n", l_strDEVISE_ISO_DECIMAL_POS);
			}
			else
			{
				if (FindElementInCURRENCYHashArrayTable(l_strInputField, &l_lPositXHashKey, &l_lPositYHashKey))
				{
					strcpy(l_strDEVISE_ISO_DECIMAL_POS, CURRENCYHashArray[l_lPositXHashKey].stElt[l_lPositYHashKey].strDECIMAL_POS);
				}
				else
				{
					// Currency not found in CURRENCYHashArray Table
					l_strDEVISE_ISO_DECIMAL_POS[0] = '3';
					l_strDEVISE_ISO_DECIMAL_POS[1] = '\0';
					printf("Currency NOT FOUND ............ : %s. Default Decimal Number = %s\n", l_strDEVISE_ISO, l_strDEVISE_ISO_DECIMAL_POS);
				}
			}
		}
		// Number of decimals of Amount : QTE_DECIMALES
		if (strcmp(tabFieldOfRecord[l_iIdx].strFieldName, "QTE_DECIMALES") == 0)
		{
			memcpy(l_strZ_MNT_ESTD_DEVISE + SIGN_FIELD_LENGTH + AMOUNT_FIELD_LENGTH, i_InputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosInput, tabFieldOfRecord[l_iIdx].iFieldLengthInput);
			// As we have not a Field to identify the Number of Decimals in the Output Amount, we force the value to AMOUNT_DECIMAL_NR = 3 in QTE_DECIMALES Field
			// memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strDEVISE_ISO_DECIMAL_POS, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
			memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, AMOUNT_DECIMAL_NR, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
		}
		// Sign : I_SIGN_MNT_DEVISE
		if (strcmp(tabFieldOfRecord[l_iIdx].strFieldName, "I_SIGN_MNT_DEVISE") == 0)
		{
			memcpy(l_strZ_MNT_ESTD_DEVISE, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
		}
		// Amount : Z_MNT_ESTD_DEVISE
		if (strcmp(tabFieldOfRecord[l_iIdx].strFieldName, "Z_MNT_ESTD_DEVISE") == 0)
		{
			// To convert Amount, we will use here the Function defined in Har_Transco_PreMai.c named CorrectFormatAmount()
			// The First  Argument of this Function is the Amount l_strZ_MNT_ESTD_DEVISE which has the folling Format : Sign, Amount, Number od decimals of the Amount
			// The Second Argument is the Number of decimals of the Currency
			// The third  Argument is the Converted Amount with AMOUNT_DECIMAL_NR decimals
			memcpy(l_strZ_MNT_ESTD_DEVISE + SIGN_FIELD_LENGTH, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
			l_strZ_MNT_ESTD_DEVISE[SIGN_FIELD_LENGTH + tabFieldOfRecord[l_iIdx].iFieldLengthOutput + 1] = '\0';
			CorrectFormatAmount(l_strZ_MNT_ESTD_DEVISE, l_strDEVISE_ISO_DECIMAL_POS, l_strOutputAmount);
			// printf(" - [Create_Output_Record] - l_strZ_MNT_ESTD_DEVISE = %s, l_strDEVISE_ISO_DECIMAL_POS = %s, l_strOutputAmount= %s\n", l_strZ_MNT_ESTD_DEVISE, l_strDEVISE_ISO_DECIMAL_POS, l_strOutputAmount);
			memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputAmount + SIGN_FIELD_LENGTH, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
		}
		
		// Check if SIAM or RTS Field in Input Record
		switch (tabFieldOfRecord[l_iIdx].iFieldType)
		{
			case	ADD_RICOS_SC_CPY_USING_SIAM : // The Field is a SIAM Field - Find RICOS_SC_ID and RICOS_CPY_ID using SIAM Field and add them in Output Record
					if (strlen(l_strInputField) > 0)
					{
						if (FindElementInSIAMHashArrayTable(l_strInputField, &l_lPositXHashKey, &l_lPositYHashKey))
						{
							l_iIdx++;
							memcpy(l_strInputField, SIAMHashArray[l_lPositXHashKey].stElt[l_lPositYHashKey].strRICOS_SC_ID,  tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
							l_strInputField[tabFieldOfRecord[l_iIdx].iFieldLengthOutput] = '\0';
							Convert_InputField (l_strInputField, &l_iIdx, l_strOutputField);
							memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
							l_iIdx++;
							memcpy(l_strInputField, SIAMHashArray[l_lPositXHashKey].stElt[l_lPositYHashKey].strRICOS_CPY_ID, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
							l_strInputField[tabFieldOfRecord[l_iIdx].iFieldLengthOutput] = '\0';
							Convert_InputField (l_strInputField, &l_iIdx, l_strOutputField);
							memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
						}
						else
						{
							l_iIdx++;
							memset(l_strOutputField, '#', tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
							memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
							l_iIdx++;
							memset(l_strOutputField, '#', tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
							memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
						}
					}
					else
					{
						l_iIdx++;
						memset(l_strOutputField, '#', tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
						memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
						l_iIdx++;
						memset(l_strOutputField, '#', tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
						memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
					}
					break;
							
			case	ADD_RICOS_SC_CPY_USING_RTS : // The Field is an RTS Field - Find RICOS_SC_ID and RICOS_CPY_ID using RTS Field and add them in Output Record
					if (strlen(l_strInputField) > 0)
					{
						if (FindElementInRTSHashArrayTable(l_strInputField, &l_lPositXHashKey, &l_lPositYHashKey))
						{
							l_iIdx++;
							memcpy(l_strInputField, RTSHashArray[l_lPositXHashKey].stElt[l_lPositYHashKey].strRICOS_SC_ID,  tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
							l_strInputField[tabFieldOfRecord[l_iIdx].iFieldLengthOutput] = '\0';
							Convert_InputField (l_strInputField, &l_iIdx, l_strOutputField);
							memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
							l_iIdx++;
							memcpy(l_strInputField, RTSHashArray[l_lPositXHashKey].stElt[l_lPositYHashKey].strRICOS_CPY_ID, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
							l_strInputField[tabFieldOfRecord[l_iIdx].iFieldLengthOutput] = '\0';
							Convert_InputField (l_strInputField, &l_iIdx, l_strOutputField);
							memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
						}
						else
						{
							l_iIdx++;
							memset(l_strOutputField, '#', tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
							memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
							l_iIdx++;
							memset(l_strOutputField, '#', tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
							memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
						}
					}
					else
					{
						l_iIdx++;
						memset(l_strOutputField, '#', tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
						memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
						l_iIdx++;
						memset(l_strOutputField, '#', tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
						memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
					}
					break;
			
			case	ADD_RICOS_SC_USING_RTS : // The Field is an RTS Field - Find RICOS_SC_ID using RTS Field and add it in Output Record
					if (strlen(l_strInputField) > 0)
					{
						if (FindElementInRTSHashArrayTable(l_strInputField, &l_lPositXHashKey, &l_lPositYHashKey))
						{
							l_iIdx++;
							memcpy(l_strInputField, RTSHashArray[l_lPositXHashKey].stElt[l_lPositYHashKey].strRICOS_SC_ID,  tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
							l_strInputField[tabFieldOfRecord[l_iIdx].iFieldLengthOutput] = '\0';
							Convert_InputField (l_strInputField, &l_iIdx, l_strOutputField);
							memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
						}
						else
						{
							l_iIdx++;
							memset(l_strOutputField, '#', tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
							memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
						}
					}
					else
					{
						l_iIdx++;
						memset(l_strOutputField, '#', tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
						memcpy(o_OutputRecord + tabFieldOfRecord[l_iIdx].iFieldStartPosOutput, l_strOutputField, tabFieldOfRecord[l_iIdx].iFieldLengthOutput);
					}							
					break;
			
			default	: // No Field to be added. The Field of the Input Record is mapped in the Output Record
					break;
		}
		l_iIdx++;
	}
	o_OutputRecord[tabFieldOfRecord[l_iIdx - 1].iFieldStartPosOutput + tabFieldOfRecord[l_iIdx - 1].iFieldLengthOutput] = '\0';
	return EXIT_OK;
}

/* 
 * =============================================================================
 *  Building Format of Output File Record using struct_pestd.conf File
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
		memset(tabFieldOfRecord[l_iFieldNumber].strFieldName, '\0', sizeof(tabFieldOfRecord[l_iFieldNumber].strFieldName));
		memset(tabFieldOfRecord[l_iFieldNumber].strFieldFormat, '\0', sizeof(tabFieldOfRecord[l_iFieldNumber].strFieldFormat));
		tabFieldOfRecord[l_iFieldNumber].iFieldFormat         = unknown;
		tabFieldOfRecord[l_iFieldNumber].iFieldType           = -1;
		tabFieldOfRecord[l_iFieldNumber].iFieldLengthInput    = -1;
		tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = -1;
		tabFieldOfRecord[l_iFieldNumber].iFieldStartPosInput  = -1;
		tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput = -1;
	}
	
	/* Opening struct_pestd.conf File */
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

	/* Reading Records of struct_pestd.conf File : Each Record gives the Characteristics of a Field of the Input File Record */
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
				l_iStartFieldPositionForInput  = l_iStartFieldPositionForInput  + tabFieldOfRecord[l_iPreviousInputFieldNumber].iFieldLengthInput + 1; // +1 because Field separator "|" in Input File
				l_iStartFieldPositionForOutput = l_iStartFieldPositionForOutput + tabFieldOfRecord[l_iFieldNumber - 1].iFieldLengthOutput;
				tabFieldOfRecord[l_iFieldNumber].iFieldStartPosInput  = l_iStartFieldPositionForInput;
				tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput = l_iStartFieldPositionForOutput;
				tabFieldOfRecord[l_iFieldNumber].iFieldLengthInput    = atoi(strtok(NULL, SEPARATOR));
				tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = tabFieldOfRecord[l_iFieldNumber].iFieldLengthInput;
			}
			if (strcmp(tabFieldOfRecord[l_iFieldNumber].strFieldName, "TIERS") == 0)
			{
				tabFieldOfRecord[l_iFieldNumber].iFieldType           = ADD_RICOS_SC_CPY_USING_SIAM;
				// Add TIERS_RICOS and TIERS_RICOS_CY in Output File
				l_iPreviousInputFieldNumber = l_iFieldNumber;
				l_iFieldNumber++;
				strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldName, "TIERS_RICOS");
				strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldFormat, "charED");
				tabFieldOfRecord[l_iFieldNumber].iFieldFormat         = whichOutputFormat(tabFieldOfRecord[l_iFieldNumber].strFieldFormat);
				tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = TIERS_RICOS_FIELD_LENGTH;
				tabFieldOfRecord[l_iFieldNumber].iFieldStartPosInput  = RICOS_SC_CPY_SIAM_INPUT_POSIT;
				tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput = tabFieldOfRecord[l_iFieldNumber - 1].iFieldStartPosOutput + tabFieldOfRecord[l_iFieldNumber -1].iFieldLengthOutput;
				l_iFieldNumber++;
				strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldName, "TIERS_RICOS_CY");
				strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldFormat, "entierZG");
				tabFieldOfRecord[l_iFieldNumber].iFieldFormat         = whichOutputFormat(tabFieldOfRecord[l_iFieldNumber].strFieldFormat);
				tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = TIERS_RICOS_FIELD_LENGTH;
				tabFieldOfRecord[l_iFieldNumber].iFieldStartPosInput  = RICOS_SC_CPY_SIAM_INPUT_POSIT;
				tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput = tabFieldOfRecord[l_iFieldNumber - 1].iFieldStartPosOutput + tabFieldOfRecord[l_iFieldNumber -1].iFieldLengthOutput;
				l_iStartFieldPositionForOutput                        = tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput;
				}
			else
			{
				if (strcmp(tabFieldOfRecord[l_iFieldNumber].strFieldName, "Z_ALIAS_ID_TIERS") == 0)
				{
					tabFieldOfRecord[l_iFieldNumber].iFieldType           = ADD_RICOS_SC_CPY_USING_RTS;
					// Add TIERS_EXT_RICOS and TIERS_EXT_RICOS_CY in Output File
					l_iPreviousInputFieldNumber = l_iFieldNumber;
					l_iFieldNumber++;
					strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldName, "TIERS_EXT_RICOS");
					strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldFormat, "charED");
					tabFieldOfRecord[l_iFieldNumber].iFieldFormat         = whichOutputFormat(tabFieldOfRecord[l_iFieldNumber].strFieldFormat);
					tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = TIERS_RICOS_FIELD_LENGTH;
					tabFieldOfRecord[l_iFieldNumber].iFieldStartPosInput  = RICOS_SC_CPY_RTS_INPUT_POSIT;
					tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput = tabFieldOfRecord[l_iFieldNumber - 1].iFieldStartPosOutput + tabFieldOfRecord[l_iFieldNumber - 1].iFieldLengthOutput;
					l_iFieldNumber++;
					strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldName, "TIERS_EXT_RICOS_CY");
					strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldFormat, "entierZG");
					tabFieldOfRecord[l_iFieldNumber].iFieldFormat         = whichOutputFormat(tabFieldOfRecord[l_iFieldNumber].strFieldFormat);
					tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = TIERS_RICOS_FIELD_LENGTH;
					tabFieldOfRecord[l_iFieldNumber].iFieldStartPosInput  = RICOS_SC_CPY_RTS_INPUT_POSIT;
					tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput = tabFieldOfRecord[l_iFieldNumber - 1].iFieldStartPosOutput + tabFieldOfRecord[l_iFieldNumber -1].iFieldLengthOutput;
					l_iStartFieldPositionForOutput                        = tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput;
				}
				else
				{
					if (strcmp(tabFieldOfRecord[l_iFieldNumber].strFieldName, "Z_ALIAS_ID_EMPPRET") == 0)
					{
						tabFieldOfRecord[l_iFieldNumber].iFieldType           = ADD_RICOS_SC_USING_RTS;
						// Add TIERS_EMPPRET_RICOS in Output File
						l_iPreviousInputFieldNumber = l_iFieldNumber;
						l_iFieldNumber++;
						strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldName, "TIERS_EMPPRET_RICOS");
						strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldFormat, "charED");
						tabFieldOfRecord[l_iFieldNumber].iFieldFormat         = whichOutputFormat(tabFieldOfRecord[l_iFieldNumber].strFieldFormat);
						tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = TIERS_RICOS_FIELD_LENGTH;
						tabFieldOfRecord[l_iFieldNumber].iFieldStartPosInput  = RICOS_SC_RTS_INPUT_POSIT;
						tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput = tabFieldOfRecord[l_iFieldNumber - 1].iFieldStartPosOutput + tabFieldOfRecord[l_iFieldNumber -1].iFieldLengthOutput;
						l_iStartFieldPositionForOutput                        = tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput;
					}
					else
					{
						if (strcmp(tabFieldOfRecord[l_iFieldNumber].strFieldName, "Z_ALIAS_ID_GARANT")  == 0)
						{
							tabFieldOfRecord[l_iFieldNumber].iFieldType           = ADD_RICOS_SC_USING_RTS;
							// Add TIERS_GARANT_RICOS in Output File
							l_iPreviousInputFieldNumber = l_iFieldNumber;
							l_iFieldNumber++;
							strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldName, "TIERS_GARANT_RICOS");
							strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldFormat, "charED");
							tabFieldOfRecord[l_iFieldNumber].iFieldFormat         = whichOutputFormat(tabFieldOfRecord[l_iFieldNumber].strFieldFormat);
							tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = TIERS_RICOS_FIELD_LENGTH;
							tabFieldOfRecord[l_iFieldNumber].iFieldStartPosInput  = RICOS_SC_RTS_INPUT_POSIT;
							tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput = tabFieldOfRecord[l_iFieldNumber - 1].iFieldStartPosOutput + tabFieldOfRecord[l_iFieldNumber -1].iFieldLengthOutput;
							l_iStartFieldPositionForOutput                        = tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput;
						}
						else
						{
							if (strcmp(tabFieldOfRecord[l_iFieldNumber].strFieldName, "Z_ALIAS_ID_EMETTIT") == 0)
							{
								tabFieldOfRecord[l_iFieldNumber].iFieldType           = ADD_RICOS_SC_USING_RTS;
								// Add TIERS_EMETTIT_RICOS in Output File
								l_iPreviousInputFieldNumber = l_iFieldNumber;
								l_iFieldNumber++;
								strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldName, "TIERS_EMETTIT_RICOS");
								strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldFormat, "charED");
								tabFieldOfRecord[l_iFieldNumber].iFieldFormat         = whichOutputFormat(tabFieldOfRecord[l_iFieldNumber].strFieldFormat);
								tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = TIERS_RICOS_FIELD_LENGTH;
								tabFieldOfRecord[l_iFieldNumber].iFieldStartPosInput  = RICOS_SC_RTS_INPUT_POSIT;
								tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput = tabFieldOfRecord[l_iFieldNumber - 1].iFieldStartPosOutput + tabFieldOfRecord[l_iFieldNumber -1].iFieldLengthOutput;
								l_iStartFieldPositionForOutput                        = tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput;
							}
							else
							{
								if (strcmp(tabFieldOfRecord[l_iFieldNumber].strFieldName, "Z_ALIAS_ID_DEPOSIT") == 0)
								{
									tabFieldOfRecord[l_iFieldNumber].iFieldType           = ADD_RICOS_SC_USING_RTS;
									// Add TIERS_DEPOSIT_RICOS in Output File
									l_iPreviousInputFieldNumber = l_iFieldNumber;
									l_iFieldNumber++;
									strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldName, "TIERS_DEPOSIT_RICOS");
									strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldFormat, "charED");
									tabFieldOfRecord[l_iFieldNumber].iFieldFormat         = whichOutputFormat(tabFieldOfRecord[l_iFieldNumber].strFieldFormat);
									tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = TIERS_RICOS_FIELD_LENGTH;
									tabFieldOfRecord[l_iFieldNumber].iFieldStartPosInput  = RICOS_SC_RTS_INPUT_POSIT;
									tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput = tabFieldOfRecord[l_iFieldNumber - 1].iFieldStartPosOutput + tabFieldOfRecord[l_iFieldNumber -1].iFieldLengthOutput;
									l_iStartFieldPositionForOutput                        = tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput;
								}
								else
								{
									if (strcmp(tabFieldOfRecord[l_iFieldNumber].strFieldName, "Z_ALIAS_ID_EMETSSJ") == 0)
									{
										tabFieldOfRecord[l_iFieldNumber].iFieldType           = ADD_RICOS_SC_USING_RTS;
										// Add TIERS_EMETSSJ_RICOS in Output File
										l_iPreviousInputFieldNumber = l_iFieldNumber;
										l_iFieldNumber++;
										strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldName, "TIERS_EMETSSJ_RICOS");
										strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldFormat, "charED");
										tabFieldOfRecord[l_iFieldNumber].iFieldFormat         = whichOutputFormat(tabFieldOfRecord[l_iFieldNumber].strFieldFormat);
										tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = TIERS_RICOS_FIELD_LENGTH;
										tabFieldOfRecord[l_iFieldNumber].iFieldStartPosInput  = RICOS_SC_RTS_INPUT_POSIT;
										tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput = tabFieldOfRecord[l_iFieldNumber - 1].iFieldStartPosOutput + tabFieldOfRecord[l_iFieldNumber -1].iFieldLengthOutput;
										l_iStartFieldPositionForOutput                        = tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput;
									}
									else
									{
										if (strcmp(tabFieldOfRecord[l_iFieldNumber].strFieldName, "Z_ALIAS_ID_ACTR") == 0)
										{
											tabFieldOfRecord[l_iFieldNumber].iFieldType           = ADD_RICOS_SC_USING_RTS;
											// Add TIERS_ACTR_RICOS in Output File
											l_iPreviousInputFieldNumber = l_iFieldNumber;
											l_iFieldNumber++;
											strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldName, "TIERS_ACTR_RICOS");
											strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldFormat, "charED");
											tabFieldOfRecord[l_iFieldNumber].iFieldFormat         = whichOutputFormat(tabFieldOfRecord[l_iFieldNumber].strFieldFormat);
											tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = TIERS_RICOS_FIELD_LENGTH;
											tabFieldOfRecord[l_iFieldNumber].iFieldStartPosInput  = RICOS_SC_RTS_INPUT_POSIT;
											tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput = tabFieldOfRecord[l_iFieldNumber - 1].iFieldStartPosOutput + tabFieldOfRecord[l_iFieldNumber -1].iFieldLengthOutput;
											l_iStartFieldPositionForOutput                        = tabFieldOfRecord[l_iFieldNumber].iFieldStartPosOutput;
										}
										else
										{
											if (strcmp(tabFieldOfRecord[l_iFieldNumber].strFieldName, "Z_ALIAS_ID_TIERORI") == 0)
											{
												tabFieldOfRecord[l_iFieldNumber].iFieldType           = ADD_RICOS_SC_USING_RTS;
												// Add TIERS_ORI_RICOS in Output File
												l_iPreviousInputFieldNumber = l_iFieldNumber;
												l_iFieldNumber++;
												strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldName, "TIERS_ORI_RICOS");
												strcpy(tabFieldOfRecord[l_iFieldNumber].strFieldFormat, "charED");
												tabFieldOfRecord[l_iFieldNumber].iFieldFormat         = whichOutputFormat(tabFieldOfRecord[l_iFieldNumber].strFieldFormat);
												tabFieldOfRecord[l_iFieldNumber].iFieldLengthOutput   = TIERS_RICOS_FIELD_LENGTH;
												tabFieldOfRecord[l_iFieldNumber].iFieldStartPosInput  = RICOS_SC_RTS_INPUT_POSIT;
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
					}
				}
			}
		}
	}
	/* Closing struct_pestd.conf File */
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
	printf("Start Har_Transco_PESTD Program ...\n");

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
	if (argc != NB_PARAM)
	{
		printf("Bad Number of Parameters. This Number must be %d instead of %d\n", NB_PARAM - 1, argc - 1);
		printf("---                                   U S A G E                                   ---\n");
		printf("   - Parameter 1 : Input File                                          [Mandatory]\n");
		printf("   - Parameter 2 : Accounting Date in YYYYMMDD Format                  [Mandatory]\n");
		return EXIT_ERR;
	}
	else
	{
		printf("Input File .................... : %s\n", argv[1]);
		printf("Accounting Date ............... : %s\n", argv[2]);
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
	
	/* Building Format of Output File Record using struct_pestd.conf File */
	if (BuildOutputRecordFormat() == EXIT_ERR)
	{
		return EXIT_ERR;
	}
	/*else
	{
		while (strlen(tabFieldOfRecord[l_lIdx].strFieldName) > 0)
		{
			printf(" - [main] - tabFieldOfRecord - Field Number %03ld : %s \t\t| %s \t\t| %01d \t\t| %03d \t\t| %04d \t\t| %04d \t\t| %04d\n", 
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

	/* Getting the REF_TIERS.dat File Format using REF_TIERS.conf File */
	// TIERS Fields with ACRO_SIAM and RTS Format converted to RICOS Format
	if (BuildRefTiersRecordFormat() == EXIT_ERR)
	{
		return EXIT_ERR;
	}

	/* Build Tables of TIERS RICOS using SIAM and RTS TIERS in REF_TIERS.dat File */
	if (BuildTiersRicosTables() == EXIT_ERR)
	{
		return EXIT_ERR;
	}
	/*else
	{
		for (l_lIdx = 0; l_lIdx < HASH_ARRAY_SIZE; l_lIdx++)
		{
			for (l_lIdY = 0; l_lIdY < HASH_ARRAY_SIZE; l_lIdY++)
			{
				if (SIAMHashArray[l_lIdx].stElt[l_lIdY].SIAMHashKey >= 0)
				{
					printf (" - [main] - SIAMHashArray - Record %03ld , %03ld : %lld | \t\t%s | \t\t%s | \t\t%s\n",	
								l_lIdx, l_lIdY,
								SIAMHashArray[l_lIdx].stElt[l_lIdY].SIAMHashKey,
								SIAMHashArray[l_lIdx].stElt[l_lIdY].strSIAM,
								SIAMHashArray[l_lIdx].stElt[l_lIdY].strRICOS_SC_ID,
								SIAMHashArray[l_lIdx].stElt[l_lIdY].strRICOS_CPY_ID);
				}
			}
		}
		printf("\n");
		for (l_lIdx = 0; l_lIdx < HASH_ARRAY_SIZE; l_lIdx++)
		{
			for (l_lIdY = 0; l_lIdY < HASH_ARRAY_SIZE; l_lIdY++)
			{
				if (RTSHashArray[l_lIdx].stElt[l_lIdY].RTSHashKey >= 0)
				{
					printf (" - [main] - RTSHashArray - Record %03ld , %03ld : %lld | \t\t%s | \t\t%s | \t\t%s\n",
								l_lIdx, l_lIdY,
								RTSHashArray[l_lIdx].stElt[l_lIdY].RTSHashKey,
								RTSHashArray[l_lIdx].stElt[l_lIdY].strRTS_SC_ID,
								RTSHashArray[l_lIdx].stElt[l_lIdY].strRICOS_SC_ID,
								RTSHashArray[l_lIdx].stElt[l_lIdY].strRICOS_CPY_ID);
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
	
	/* Handling Input Data and Creating Output File */
	while (fgets((char*) &Input_Record_Struct, MAX_INPUT_REC_LENGTH, InputFile_Ptr) != NULL)
	{
		if (strlen(Input_Record_Struct.Input_Record) > 1)
		{
			// Handle only not empty records
			Record_Number++;
			// Create Output Record Header
			memset(Output_Record_Struct.Output_Header, ' ', HEADER_LENGTH);
			memcpy(Output_Record_Struct.Output_Header,  HEADER_MVT_LABEL, strlen(HEADER_MVT_LABEL));
			memcpy(Output_Record_Struct.Output_Header + HEADER_MVT_LABEL_LENGTH, Accounting_Date, DATE_LENGTH);
			// Create Output Record Detail
			memset(Output_Record_Struct.Output_Record, ' ', MAX_INPUT_REC_LENGTH + TIERS_RICOS_FIELD_NUMBER * TIERS_RICOS_FIELD_LENGTH);
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
			Empty_Record_Number++;
		}
	}
	
	printf("Total Number of Records Read .. : %d\n", Record_Number + Empty_Record_Number);
	printf("Total Number of Handled Records : %d\n", Record_Number);

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
	printf("End   Har_Transco_PESTD Program ...\n");
}
