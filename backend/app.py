import os
import tempfile
import uuid
import re
from datetime import datetime, timedelta
import shutil
import pdfplumber
import pandas as pd
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename
import PyPDF2
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ["https://extractmsds.vercel.app"]}})


UPLOAD_FOLDER = os.path.join(tempfile.gettempdir(), 'sds_uploads')
PROCESSED_FOLDER = os.path.join(tempfile.gettempdir(), 'sds_processed')
ALLOWED_EXTENSIONS_PDF = {'pdf'}
ALLOWED_EXTENSIONS_EXCEL = {'xlsx', 'xls'}

# Create directories if they don't exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_FOLDER, exist_ok=True)

# Required columns with CAS Number explicitly included
COLUMNS = [
    "Description",
    "CAS Number",
    "Material Name",
    "Trade Name",
    "Physical state",
    "Static Hazard",
    "Vapour Pressure",
    "Flash Point (°C)",
    "Flammable Limits by Volume (LEL, UEL)",
    "Melting Point (°C)",
    "Boiling Point (°C)",
    "Density",
    "Relative Vapour Density (Air = 1)",
    "Ignition Temperature (°C)",
    "Threshold Limit Value (ppm)",
    "Immediate Danger to Life in Humans",
    "LD50 (mg/kg)",
    "LC50",
    "Source of Information"
]

def allowed_file(filename, allowed_extensions):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extensions

def extract_pdf_text_fallback(pdf_path):
    """Extract text from PDF using PyPDF2 as fallback"""
    try:
        text = ""
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            for page in pdf_reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        return text
    except Exception as e:
        logger.error(f"PyPDF2 extraction failed for {pdf_path}: {str(e)}")
        return ""

def extract_pdf_text(pdf_path):
    """Extract text from PDF file with fallback methods"""
    text = ""
    
    # Try pdfplumber first
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        
        if text.strip():
            logger.info(f"Successfully extracted text using pdfplumber from {os.path.basename(pdf_path)}")
            return text
    except Exception as e:
        logger.warning(f"pdfplumber failed for {pdf_path}: {str(e)}")
    
    # Fallback to PyPDF2
    text = extract_pdf_text_fallback(pdf_path)
    if text.strip():
        logger.info(f"Successfully extracted text using PyPDF2 from {os.path.basename(pdf_path)}")
        return text
    
    logger.error(f"All text extraction methods failed for {pdf_path}")
    return ""

def clean_numeric_value(value_str):
    """Clean and standardize numeric values"""
    if not value_str or value_str.lower() in ['nda', 'n/a', 'not available']:
        return "NDA"
    
    # Remove extra whitespace and common prefixes
    value_str = value_str.strip()
    value_str = re.sub(r'^[:\-\s]+', '', value_str)
    
    # Extract first number found
    number_match = re.search(r'([\d,]+[.,]?\d*)', value_str)
    if number_match:
        number = number_match.group(1)
        # Convert comma decimal separator to dot
        number = re.sub(r'(\d+),(\d+)$', r'\1.\2', number)
        return number
    
    return "NDA"

def extract_flammable_limits(text):
    """
    Extract flammable limits (LEL and UEL) with comprehensive pattern matching
    Returns formatted string like "LEL: X%, UEL: Y%" or individual values
    """
    
    # Initialize variables
    lel_value = None
    uel_value = None
    
    # Comprehensive patterns for flammable limits
    flammable_patterns = [
        # Pattern 1: LEL and UEL on same line with percentages
        r"LEL[:\s]*(\d+(?:\.\d+)?)\s*%.*?UEL[:\s]*(\d+(?:\.\d+)?)\s*%",
        r"Lower\s+explosive\s+limit[:\s]*(\d+(?:\.\d+)?)\s*%.*?Upper\s+explosive\s+limit[:\s]*(\d+(?:\.\d+)?)\s*%",
        r"LFL[:\s]*(\d+(?:\.\d+)?)\s*%.*?UFL[:\s]*(\d+(?:\.\d+)?)\s*%",
        
        # Pattern 2: Range format like "2.1 - 12.8%" or "2.1% - 12.8%"
        r"(?:LEL|Lower\s+explosive\s+limit|LFL|Flammable\s+limits?)[:\s]*(\d+(?:\.\d+)?)\s*%?\s*[-–—]\s*(\d+(?:\.\d+)?)\s*%",
        r"Explosive\s+limits?[:\s]*(\d+(?:\.\d+)?)\s*%?\s*[-–—]\s*(\d+(?:\.\d+)?)\s*%",
        r"Flammability\s+limits?[:\s]*(\d+(?:\.\d+)?)\s*%?\s*[-–—]\s*(\d+(?:\.\d+)?)\s*%",
        
        # Pattern 3: Parentheses format like "(2.1 - 12.8%)" or "(LEL: 2.1%, UEL: 12.8%)"
        r"\(\s*(\d+(?:\.\d+)?)\s*%?\s*[-–—]\s*(\d+(?:\.\d+)?)\s*%?\s*\)",
        r"\(\s*LEL[:\s]*(\d+(?:\.\d+)?)\s*%.*?UEL[:\s]*(\d+(?:\.\d+)?)\s*%\s*\)",
        
        # Pattern 4: Table-like format with vol% or volume%
        r"(?:LEL|Lower)[:\s]*(\d+(?:\.\d+)?)\s*(?:vol\s*%|%\s*vol|%|volume\s*%).*?(?:UEL|Upper)[:\s]*(\d+(?:\.\d+)?)\s*(?:vol\s*%|%\s*vol|%|volume\s*%)",
        
        # Pattern 5: Simple numeric range without explicit LEL/UEL labels but in flammable context
        r"(?:Flammable|Explosive)\s+(?:range|limits?)[:\s]*(\d+(?:\.\d+)?)\s*[-–—]\s*(\d+(?:\.\d+)?)\s*%",
        
        # Pattern 6: Individual LEL/UEL on separate lines
        r"LEL[:\s]*(\d+(?:\.\d+)?)\s*%",
        r"UEL[:\s]*(\d+(?:\.\d+)?)\s*%",
    ]
    
    # Try patterns that capture both LEL and UEL
    for pattern in flammable_patterns[:5]:  # First 5 patterns capture both values
        matches = re.findall(pattern, text, re.IGNORECASE | re.MULTILINE | re.DOTALL)
        if matches:
            match = matches[0]
            if len(match) == 2:
                lel_value = match[0].strip()
                uel_value = match[1].strip()
                logger.debug(f"Found LEL/UEL pair with pattern '{pattern}': LEL={lel_value}%, UEL={uel_value}%")
                break
    
    # If we didn't find a pair, try individual patterns
    if not lel_value or not uel_value:
        # Look for individual LEL
        lel_patterns = [
            r"LEL[:\s]*(\d+(?:\.\d+)?)\s*%",
            r"Lower\s+explosive\s+limit[:\s]*(\d+(?:\.\d+)?)\s*%",
            r"LFL[:\s]*(\d+(?:\.\d+)?)\s*%",
            r"Lower\s+flammable\s+limit[:\s]*(\d+(?:\.\d+)?)\s*%"
        ]
        
        for pattern in lel_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                lel_value = matches[0].strip()
                logger.debug(f"Found individual LEL: {lel_value}%")
                break
        
        # Look for individual UEL
        uel_patterns = [
            r"UEL[:\s]*(\d+(?:\.\d+)?)\s*%",
            r"Upper\s+explosive\s+limit[:\s]*(\d+(?:\.\d+)?)\s*%",
            r"UFL[:\s]*(\d+(?:\.\d+)?)\s*%",
            r"Upper\s+flammable\s+limit[:\s]*(\d+(?:\.\d+)?)\s*%"
        ]
        
        for pattern in uel_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                uel_value = matches[0].strip()
                logger.debug(f"Found individual UEL: {uel_value}%")
                break
    
    # Format the result
    if lel_value and uel_value:
        result = f"LEL: {lel_value}%, UEL: {uel_value}%"
    elif lel_value:
        result = f"LEL: {lel_value}%"
    elif uel_value:
        result = f"UEL: {uel_value}%"
    else:
        # Check for explicit "not applicable" or "non-flammable" statements
        non_flammable_patterns = [
            r"not\s+flammable",
            r"non[-\s]?flammable",
            r"flammable\s+limits?\s*:?\s*(?:not\s+applicable|n/?a)",
            r"explosive\s+limits?\s*:?\s*(?:not\s+applicable|n/?a)",
            r"does\s+not\s+burn",
            r"will\s+not\s+burn",
            r"non[-\s]?combustible"
        ]
        
        for pattern in non_flammable_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                logger.debug(f"Found non-flammable indicator: {pattern}")
                return "Non-flammable"
        
        logger.debug("No flammable limits found")
        result = "NDA"
    
    return result


def parse_sds_data(text, source_filename):
    """Enhanced SDS data extraction with comprehensive pattern matching and debugging"""
    logger.info(f"Parsing SDS data from {source_filename}")
    
    # Add debug logging
    logger.debug(f"Text length: {len(text)} characters")
    logger.debug(f"First 500 chars: {text[:500]}")
    
    def find_between(pattern, default="NDA", field_name=""):
        matches = re.findall(pattern, text, re.IGNORECASE | re.MULTILINE)
        if matches:
            result = matches[0].strip() if isinstance(matches[0], str) else str(matches[0]).strip()
            result = clean_numeric_value(result) if any(char.isdigit() for char in result) else result
            logger.debug(f"Found {field_name}: {result}")
            return result
        logger.debug(f"No match found for {field_name}")
        return default
    flammable_limits = extract_flammable_limits(text)
    # Fixed CAS Number extraction - handles complete CAS number format
    def extract_cas_number(text):
        """Extract CAS number without applying numeric cleaning"""
        cas_patterns = [
            r"CAS-No\.?\s*[:\-]?\s*[\[\(]?\s*(\d{2,7}-\d{2}-\d)\s*[\]\)]?",
            r"CAS\s+No\.?\s*[:\-]?\s*[\[\(]?\s*(\d{2,7}-\d{2}-\d)\s*[\]\)]?",
            r"CAS\s+number\s*[:\-]?\s*[\[\(]?\s*(\d{2,7}-\d{2}-\d)\s*[\]\)]?",
            r"CAS#?\s*[:\-]?\s*[\[\(]?\s*(\d{2,7}-\d{2}-\d)\s*[\]\)]?",
            r"【CAS】\s*[:\-]?\s*[\[\(]?\s*(\d{2,7}-\d{2}-\d)\s*[\]\)]?",
            # More flexible pattern for various CAS formats
            r"(?:CAS|cas)(?:\s*-?\s*(?:No|NUMBER|#))?\s*[:\-]?\s*[\[\(]?\s*(\d{2,7}-\d{2}-\d)\s*[\]\)]?",
            # Pattern for standalone CAS numbers (more restrictive to avoid false positives)
            r"\b(\d{2,7}-\d{2}-\d)\b"
        ]
        
        for pattern in cas_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE | re.MULTILINE)
            if matches:
                cas_result = matches[0].strip()
                logger.debug(f"Found CAS Number with pattern '{pattern}': {cas_result}")
                return cas_result
        
        logger.debug("No CAS Number found")
        return "NDA"
    
    def extract_static_hazard(text):
        """Extract static hazard information - return Yes/No/NDA based on static discharge mentions"""
        # First check if there's any mention of static-related topics at all
        static_patterns = [
            r"static\s+discharge",
            r"electrostatic\s+discharge",
            r"static\s+electricity",
            r"electrostatic\s+charge",
            r"static\s+charge",
            r"precautionary\s+measures\s+against\s+static\s+discharge",
            r"measures\s+to\s+prevent.*static",
            r"ground.*bond.*container",
            r"grounding.*bonding",
            r"anti[-\s]?static",
            r"static\s+sensitive",
            r"electrostatic\s+ignition",
            r"static\s+buildup"
        ]
        
        # Patterns that indicate NO static hazard
        no_static_patterns = [
            r"no\s+static\s+hazard",
            r"static\s+hazard\s*:?\s*no",
            r"not\s+static\s+sensitive",
            r"no\s+electrostatic\s+hazard",
            r"static\s+discharge\s*:?\s*not\s+applicable",
            r"static\s+discharge\s*:?\s*n/?a"
        ]
        
        # Check for explicit "No" indicators first
        for pattern in no_static_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                logger.debug(f"Found explicit no static hazard indicator: {pattern}")
                return "No"
        
        # Check for "Yes" indicators
        for pattern in static_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                logger.debug(f"Found static hazard indicator: {pattern}")
                return "Yes"
        
        # Check if there's any mention of handling/storage sections where static info might be expected
        handling_sections = [
            r"SECTION\s*7.*?(?:handling|storage)",
            r"handling\s+and\s+storage",
            r"precautions\s+for\s+safe\s+handling",
            r"storage\s+conditions"
        ]
        
        has_handling_section = False
        for pattern in handling_sections:
            if re.search(pattern, text, re.IGNORECASE):
                has_handling_section = True
                break
        
        if has_handling_section:
            # If there's a handling section but no static hazard info, assume "No"
            logger.debug("Found handling/storage section but no static hazard info - assuming No")
            return "No"
        else:
            # If no handling section found, data is not available
            logger.debug("No static hazard information or handling section found")
            return "NDA"
    
    
    
    cas_number = extract_cas_number(text)
    
    # Use PDF filename as description (remove .pdf extension)
    desc = os.path.splitext(source_filename)[0]
    
    # Enhanced pattern matching for various properties
    physical_state = find_between(
        r"""(?ix)
            \b(?:physical\s+state|appearance:\s+form|appearance)              # keywords
            (?:\s+at\s+[^\n\r:]*?(?:degree|°)\s*[CF])?           # optional: at 25 degreeC or °C etc.
            \s*[:\-]?\s*                                         # optional colon or dash
            ([^\n\r.]+)                                          # capture physical state
        """,
        "NDA",
        "Physical State"
    )

    
    
    static_hazard = extract_static_hazard(text)


    vapour_pressure = "NDA"
    
    vp_pattern = r"""(?ix)                             # (?i) case-insensitive, (?x) verbose mode
        vapo[u]?r\s+pressure                           # 'vapor pressure' or 'vapour pressure'
        (?:\s*\(.*?\))?                                # optional: (mmHg), (Pa), etc.
        (?:\s+at\s+\d{1,3}\s*(?:degree)?\s*[°]?[Cc])?   # optional: 'at 20 degreeC' or 'at 30 C'
        \s*[:\-]?\s*                                   # optional: colon, dash
        (.*)                                           # capture the rest of the line
    """
    
    match = re.search(vp_pattern, text)
    if match:
        vapour_pressure = match.group(1).strip()


    
    # Temperature for vapor pressure
    # temp_match = re.search(r"(?:at|@)\s*(\d+)\s*°?C", text, re.IGNORECASE)
    # if temp_match:
        #vapour_temp = temp_match.group(1)
    
    # Extract other properties with multiple patterns

    
    trade_pattern = r"(?i)(?:Product\s*/\s*)?Trade\s*Name(?:\s*&\s*Synonyms)?\s*:?\s*([^\n\r]+)"
    m = re.search(trade_pattern, text)
    
    if m:
        trade_name = m.group(1).strip()
    else:
        trade_name = "NDA"


    flash_point = find_between(
        r"""(?ix)                            # Case-insensitive, verbose mode
        \bflash\s+point\b                   # Match 'Flash point'
        \s*:?                               # Optional colon, no dash
        \s*                                 # Optional space after colon
        (.*)                                # Capture everything (including negative sign)
        """,
        "NDA",
        "Flash Point"
    )



    
    melting_point = find_between(r"Melting\s+point\s*:?\s*([\d\-,]+[.,]?\d*)", "NDA", "Melting Point")
    boiling_point = find_between(
        r"(?i)\b(?:boiling\s*point(?:\s*(?:or|,)?\s*initial\s*boiling\s*point(?:\s*and\s*boiling\s*range)?)?|initial\s*boiling\s*point(?:\s*and\s*boiling\s*range)?|boiling\s*point\s*/\s*range|boiling\s*point\s*/\s*boiling\s*range|boiling\s*point\s*,?\s*range|boiling\s*point\s*\(.*?\))\b[:\s\-]*([^\n\r]*)",
        "NDA",
        "Boiling Point"
    )
    
    density = "NDA"
    
    # Define values to ignore
    invalid_values = ["not measured", "no data available", "Not applicable", "No data available", "not applicable","not available"]

    match = re.search(r"Density\s+and\s+/\s+or\s+relative\s+density\s*[:\-]?\s*(.*)", text, re.IGNORECASE)
    if match:
        value = match.group(1).strip()
        if value.lower() not in invalid_values:
            density = value
    
    # Priority 1: Try 'Relative Density' (case-insensitive)
    match = re.search(r"Relative\s+Density\s*[:\-]?\s*(.*)", text)
    if match:
        value = match.group(1).strip()
        if value.lower() not in invalid_values:
            density = value
    
    # Priority 2: Try 'Density' (case-insensitive)
    if density == "NDA":
        match = re.search(r"Density\s*[:\-]?\s*(.*)", text)
        if match:
            value = match.group(1).strip()
            if value.lower() not in invalid_values:
                density = value
    
    # Priority 3: Fallbacks (case-insensitive with multiple patterns)
    if density == "NDA":
        fallback_pattern = r"""(?ix)
            (    
                Specific\s+gravity\s*\(.*?=\s*1\)
                |
                Relative\s+density\s*\(.*?=\s*1\)
                |
                Specific\s+gravity(?:\s+density)?
                |
                Specific\s+gravity\s*/\s*density
                |
                Density\s*/\s*Specific\s+gravity
                |
                Density\s+at\s+\d{1,3}\s*(?:°|degree)?\s*[CFK]
                |
                Density\s*@\s*\d{1,3}\s*[CFK]
                |
                Density\s+at\s+\d{1,3}\s*(?:°|degree)?\s*[CFK]\s*,\s*[gkmg/^\s\d.]+
                |
                Specific\s+gravity\s+at\s+\d{1,3}\s*(?:°|degree)?\s*[CFK]
                |
                Relative\s+density\s+at\s+\d{1,3}\s*(?:°|degree)?\s*[CFK]
                
            )
            \s*[:\-]?\s*
            (.*)
        """
        match = re.search(fallback_pattern, text)
        if match:
            value = match.group(2).strip()
            if value.lower() not in invalid_values:
                density = value
    

    # vapor density
    vapor_density = "NDA"

    vapor_density_pattern = r"""(?ix)                                 # (?i) case-insensitive, (?x) verbose mode
        (?:relative\s+)?                                               # optional 'relative'
        vapo[u]?r\s+density                                            # 'vapor density' or 'vapour density'
        (?:\s*\(air\s*=\s*1\))?                                        # optional: (air = 1)
        (?:\s+at\s+\d{1,3}\s*(?:degree)?\s*[°]?\s*C)?                  # optional: at 20 °C or at 30 degree C
        \s*[:\-]?\s*                                                   # optional colon or dash
        (.*)                                                           # capture everything after the label
    """
    
    match = re.search(vapor_density_pattern, text)
    if match:
        vapor_density = match.group(1).strip()

    
   

    # LD50 extraction
    ld50_patterns = [
        r"LD[50₅O]+\s*(?:oral|dermal)?\s*[:\-]?\s*([><=]?\s*\d+(?:[.,]\d+)?\s*(?:mg|g)/kg(?:\s*\(.*?\))?)",
        r"LD[50₅O]+\s*[:\-]?\s*(?:oral|dermal)?\s*([><=]?\s*\d+(?:[.,]\d+)?\s*(?:mg|g)/kg(?:\s*\(.*?\))?)",
        r"LD[50₅O]+\s*(?:oral|dermal)?\s*([><=]?\s*\d+(?:[.,]\d+)?\s*(?:mg|g)/kg(?:\s*\(.*?\))?)",
    ]

    ld50 = "NDA"
    for pattern in ld50_patterns:
        ld50 = find_between(pattern, "NDA", "LD50")
        if ld50 != "NDA":
            break
    lc50_patterns = [
        r"LC[50₅O]+\s*(?:inhalation)?\s*[:\-]?\s*([><=]?\s*\d+(?:[.,]\d+)?\s*(?:mg|g)/L(?:\s*\((?!.*fish|zebrafish|minnow).*?\))?(?:\s*\d+\s*(?:h|hr))?)",
        r"LC[50₅O]+\s*(?:inhalation)?\s*[:\-]?\s*([><=]?\s*\d+(?:[.,]\d+)?\s*ppm(?:\s*\((?!.*fish|zebrafish|minnow).*?\))?(?:\s*\d+\s*(?:h|hr))?)"
    ]
    
    lc50 = "NDA"
    for pattern in lc50_patterns:
        lc50 = find_between(pattern, "NDA", "LC50")
        if lc50 != "NDA":
            break

                
    chemical_name_patterns = [
        r"(?i)Material name[:\s]*([^\n\r]+)",
        r"(?i)Product name[:\s]*([^\n\r]+)",
        r"(?i)Product names[:\s]*([^\n\r]+)",
        r"(?i)Product Name:[:\s]*([^\n\r]+)",
        r"(?i)Product name\s*:[:\s]*([^\n\r]+)",
        r"(?i)Product description[:\s]*([^\n\r]+)",
        r"(?i)Identification of the substance[:\s]*([^\n\r]+)",
    ]


    name = "NDA"

    for pattern in chemical_name_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            extracted = match.group(1).strip()

            # ✅ Skip if it's clearly a section header (too long or contains slashes)
            if len(extracted) > 60 or "/" in extracted.lower() or "company" in extracted.lower():
                continue

            name = extracted
            break

    
    pattern = r"""(?ix)                                                        
        (?:auto|self)?                                               
        [-\s]?                                                       
        ignition                                                     
        (?:\s*temperature)?                                         
        (?:\s*,?\s*°?\s*C)?                                         
        \s*[:\-]?\s*                                                
        ([\d,]+[.,]?\d*)                                            
    """
    match = re.search(pattern, text)
    ignition_temp = match.group(1) if match else "NDA"


    extracted_data = {
        "Description": desc,
        "CAS Number": cas_number,
        "Material Name": name,
        "Trade Name": trade_name,
        "Physical state": physical_state,
        "Static Hazard": static_hazard,
        "Vapour Pressure": vapour_pressure,
        "Flash Point (°C)": flash_point,
        "Flammable Limits by Volume (LEL, UEL)": extract_flammable_limits(text),
        "Melting Point (°C)": melting_point,
        "Boiling Point (°C)": boiling_point,
        "Density": density,
        "Relative Vapour Density (Air = 1)": vapor_density,
        "Ignition Temperature (°C)": ignition_temp,
        "Threshold Limit Value (ppm)": find_between(r"TLV\s*:?\s*([^\n\r]+)", "NDA", "TLV"),
        "Immediate Danger to Life in Humans": find_between(r"LC50\s*[-:]\s*.*?([0-9,]+.*?)\s*(mg|g|ppm|mL|L)", "NDA"),
        "LD50 (mg/kg)": ld50,
        "LC50": lc50,
        "Source of Information": "MSDS"
    }
    
    # Log extracted data for debugging
    logger.info(f"Extracted data for {source_filename}:")
    for key, value in extracted_data.items():
        if value != "NDA":
            logger.info(f"  {key}: {value}")
    
    return extracted_data

def merge_by_cas_number_optional(rows, merge_duplicates=False):
    """
    Optionally group SDS entries by CAS number and merge each group into a single row.
    If merge_duplicates is False, returns all rows without merging.
    """
    if not rows:
        return []
    
    if not merge_duplicates:
        logger.info(f"Keeping all {len(rows)} entries without merging")
        return rows
    
    merged = {}
    
    for row in rows:
        cas_key = row.get("CAS Number", "").strip()
        if cas_key.lower() in ["nda", "", "n/a"]:
            # For entries without CAS numbers, use description as key to avoid merging
            cas_key = f"no_cas_{row.get('Description', 'unknown')}_{len(merged)}"
        
        if cas_key not in merged:
            merged[cas_key] = row.copy()
        else:
            # Merge data, preferring non-NDA values
            for col in COLUMNS:
                current = merged[cas_key].get(col, "NDA")
                new = row.get(col, "NDA")
                if current in ["", "NDA", None, "n/a"] and new not in ["", "NDA", None, "n/a"]:
                    merged[cas_key][col] = new
    
    logger.info(f"Merged {len(rows)} entries into {len(merged)} unique entries")
    return list(merged.values())

def check_for_duplicates(existing_df, new_data_df, duplicate_check_mode="description"):
    """
    Check for duplicates based on different criteria.
    
    duplicate_check_mode options:
    - "none": No duplicate checking, add all entries
    - "cas": Check by CAS number only
    - "description": Check by description (filename) only  
    - "both": Check by both CAS number and description
    """
    if duplicate_check_mode == "none":
        return new_data_df
    
    if len(existing_df) == 0:
        return new_data_df
    
    # Create filters based on mode
    duplicate_filters = []
    
    if duplicate_check_mode in ["cas", "both"]:
        # Filter by CAS Number
        if "CAS Number" in existing_df.columns:
            existing_cas = set(existing_df["CAS Number"].dropna().astype(str).str.strip().str.lower())
            existing_cas.discard("nda")  # Remove NDA entries from duplicate check
            cas_filter = ~new_data_df["CAS Number"].str.strip().str.lower().isin(existing_cas)
            duplicate_filters.append(cas_filter)
    
    if duplicate_check_mode in ["description", "both"]:
        # Filter by Description
        if "Description" in existing_df.columns:
            existing_desc = set(existing_df["Description"].dropna().astype(str).str.strip().str.lower())
            desc_filter = ~new_data_df["Description"].str.strip().str.lower().isin(existing_desc)
            duplicate_filters.append(desc_filter)
    
    # Apply filters
    if duplicate_filters:
        if duplicate_check_mode == "both":
            # For "both" mode, entry must be new in BOTH CAS and description
            combined_filter = duplicate_filters[0] & duplicate_filters[1]
        else:
            # For single criteria, use that filter
            combined_filter = duplicate_filters[0]
        
        filtered_df = new_data_df[combined_filter]
        logger.info(f"Duplicate check ({duplicate_check_mode}): {len(new_data_df)} -> {len(filtered_df)} entries")
        return filtered_df
    
    return new_data_df

@app.route('/')
def index():
    return jsonify({
        'message': 'SDS Processing API Server',
        'status': 'running',
        'version': '2.1',
        'endpoints': {
            'upload': 'POST /api/upload',
            'download': 'GET /api/download/<session_id>/<filename>',
            'cleanup': 'POST /api/cleanup',
            'health': 'GET /api/health'
        }
    })

@app.route('/api/upload', methods=['POST', 'OPTIONS'])

def upload_files():
    
    if request.method == 'OPTIONS':
        return '', 200  # This handles the CORS preflight request

    try:
        logger.info("Processing upload request")
        
        if 'pdfFiles' not in request.files or 'excelFile' not in request.files:
            return jsonify({'error': 'Missing required files (pdfFiles or excelFile)'}), 400
        
        pdf_files = request.files.getlist('pdfFiles')
        excel_file = request.files['excelFile']
        
        # Get processing options from form data
        merge_duplicates = request.form.get('mergeDuplicates', 'false').lower() == 'true'
        duplicate_check = request.form.get('duplicateCheck', 'none')  # none, cas, description, both
        
        logger.info(f"Processing options: merge_duplicates={merge_duplicates}, duplicate_check={duplicate_check}")
        
        if not pdf_files or excel_file.filename == '':
            return jsonify({'error': 'No files selected'}), 400
        
        logger.info(f"Received {len(pdf_files)} PDF files and 1 Excel file")
        
        # Validate file extensions
        for pdf_file in pdf_files:
            if not allowed_file(pdf_file.filename, ALLOWED_EXTENSIONS_PDF):
                return jsonify({'error': f'Invalid PDF file format: {pdf_file.filename}'}), 400
        
        if not allowed_file(excel_file.filename, ALLOWED_EXTENSIONS_EXCEL):
            return jsonify({'error': 'Invalid Excel file format'}), 400
        
        # Create unique session ID for this upload
        session_id = str(uuid.uuid4())
        session_dir = os.path.join(UPLOAD_FOLDER, session_id)
        os.makedirs(session_dir, exist_ok=True)
        
        # Save uploaded files
        pdf_paths = []
        for pdf_file in pdf_files:
            pdf_path = os.path.join(session_dir, secure_filename(pdf_file.filename))
            pdf_file.save(pdf_path)
            pdf_paths.append(pdf_path)
            logger.info(f"Saved PDF: {pdf_file.filename}")
        
        excel_path = os.path.join(session_dir, secure_filename(excel_file.filename))
        excel_file.save(excel_path)
        logger.info(f"Saved Excel: {excel_file.filename}")
        
        # Process PDF files and extract SDS data
        all_data = []
        processed_files = 0
        skipped_files = []
        
        for pdf_path in pdf_paths:
            filename = os.path.basename(pdf_path)
            try:
                logger.info(f"Processing {filename}...")
                text = extract_pdf_text(pdf_path)
                
                if text.strip():  # Only process if we got text
                    parsed_data = parse_sds_data(text, filename)
                    all_data.append(parsed_data)
                    processed_files += 1
                    logger.info(f"Successfully processed {filename}")
                else:
                    skipped_files.append(f"{filename} (no text extracted)")
                    logger.warning(f"No text extracted from {filename}")
            except Exception as e:
                error_msg = f"Error processing {filename}: {str(e)}"
                logger.error(error_msg)
                skipped_files.append(f"{filename} (processing error: {str(e)})")
        
        if not all_data:
            return jsonify({'error': 'No valid SDS data could be extracted from any PDF files. Please check if the PDFs contain readable text.'}), 400
        
        logger.info(f"Extracted data from {processed_files} files")
        
        # Optionally merge data by CAS Number
        processed_data = merge_by_cas_number_optional(all_data, merge_duplicates)
        
        # Create DataFrame with proper column structure
        new_data_df = pd.DataFrame(processed_data)
        
        # Ensure all required columns exist
        for col in COLUMNS:
            if col not in new_data_df.columns:
                new_data_df[col] = "NDA"
        
        # Reorder columns
        new_data_df = new_data_df[COLUMNS]
        
        # Read existing Excel file
        try:
            existing_df = pd.read_excel(excel_path)
            logger.info(f"Read existing Excel with {len(existing_df)} rows")
            
            # Ensure existing DataFrame has all required columns
            for col in COLUMNS:
                if col not in existing_df.columns:
                    existing_df[col] = "NDA"
            
            # Reorder columns to match COLUMNS order
            existing_df = existing_df.reindex(columns=COLUMNS, fill_value="NDA")
            
            # Check for duplicates based on specified criteria
            new_entries = check_for_duplicates(existing_df, new_data_df, duplicate_check)
            
            # Combine existing and new data
            if len(new_entries) > 0:
                combined_df = pd.concat([existing_df, new_entries], ignore_index=True)
            else:
                combined_df = existing_df
                
        except Exception as e:
            logger.error(f"Error reading Excel file: {str(e)}")
            # If we can't read the existing file, just use the new data
            combined_df = new_data_df
            new_entries = new_data_df
        
        # Save to new Excel file
        output_filename = "extracted_msds.xlsx"
        output_path = os.path.join("/tmp", output_filename)

        
        # Save with proper formatting
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            combined_df.to_excel(writer, index=False, sheet_name='SDS_Data')
            
        logger.info(f"Saved updated Excel file: {output_filename}")
        
        # Prepare response message
        message = f'Successfully processed {processed_files} PDF files'
        if 'new_entries' in locals() and len(new_entries) > 0:
            message += f', added {len(new_entries)} new entries'
        else:
            message += ', no new entries added'
            if duplicate_check != "none":
                message += f' (duplicate check: {duplicate_check})'
            
        if merge_duplicates and len(processed_data) != len(all_data):
            message += f' (merged {len(all_data)} entries into {len(processed_data)} unique entries by CAS Number)'
        
        response_data = {
            'success': True,
            'message': message,
            'outputFile': output_filename,
            'sessionId': session_id,
            'processedFiles': processed_files,
            'totalFiles': len(pdf_files),
            'newEntriesAdded': len(new_entries) if 'new_entries' in locals() else len(new_data_df),
            'totalEntriesInOutput': len(combined_df),
            'processingOptions': {
                'mergeDuplicates': merge_duplicates,
                'duplicateCheck': duplicate_check
            }
        }
        
        if skipped_files:
            response_data['skippedFiles'] = skipped_files
        
        return jsonify(response_data)
    
    except Exception as e:
        error_msg = f"Upload processing error: {str(e)}"
        logger.error(error_msg)
        return jsonify({'error': error_msg}), 500

@app.route('/api/download/<filename>', methods=['GET'])
def download_file(filename):
    try:
        file_path = os.path.join("/tmp", secure_filename(filename))
        if os.path.exists(file_path):
            return send_file(file_path, as_attachment=True)
        else:
            return jsonify({'error': 'File not found'}), 404
    except Exception as e:
        return jsonify({'error': f'Error downloading file: {str(e)}'}), 500



@app.route('/api/cleanup', methods=['POST'])
def cleanup_old_files():
    try:
        # Remove files older than 24 hours
        cutoff_time = datetime.now() - timedelta(hours=24)
        cleaned_sessions = 0
        cleaned_files = 0
        
        # Clean up upload folder
        if os.path.exists(UPLOAD_FOLDER):
            for session_id in os.listdir(UPLOAD_FOLDER):
                session_dir = os.path.join(UPLOAD_FOLDER, session_id)
                if os.path.isdir(session_dir):
                    try:
                        created_time = datetime.fromtimestamp(os.path.getctime(session_dir))
                        if created_time < cutoff_time:
                            shutil.rmtree(session_dir, ignore_errors=True)
                            cleaned_sessions += 1
                    except Exception as e:
                        logger.error(f"Error cleaning session {session_id}: {str(e)}")
        
        # Clean up processed folder
        tmp_dir = '/tmp'
        if os.path.exists(tmp_dir):
            for filename in os.listdir(tmp_dir):
                file_path = os.path.join(tmp_dir, filename)
                try:
                    created_time = datetime.fromtimestamp(os.path.getctime(file_path))
                    if created_time < cutoff_time:
                        os.remove(file_path)
                        cleaned_files += 1
                except Exception as e:
                    logger.error(f"Error cleaning file {filename}: {str(e)}")
        
        return jsonify({
            'success': True, 
            'message': f'Cleanup completed: {cleaned_sessions} sessions and {cleaned_files} files removed'
        })
    
    except Exception as e:
        return jsonify({'error': f'Error during cleanup: {str(e)}'}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'upload_folder': UPLOAD_FOLDER,
        'processed_folder': PROCESSED_FOLDER,
        'version': '2.1'
    })

if __name__ == '__main__':
    print("🚀 Starting Enhanced SDS Processing Flask Server v2.1...")
    print(f"📁 Upload folder: {UPLOAD_FOLDER}")
    print(f"📁 Processed folder: {PROCESSED_FOLDER}")
    print("🌐 Server will be available at http://localhost:5000")
    print("✨ Now supports multiple entries with same CAS number")
    print("🎛️  Processing options:")
    print("   - mergeDuplicates: Merge entries with same CAS number")
    print("   - duplicateCheck: none|cas|description|both")
    app.run(debug=True, host='0.0.0.0', port=5000)
