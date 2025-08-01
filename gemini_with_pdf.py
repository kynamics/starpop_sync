import os
import json
import io
import google.generativeai as genai
from jsonschema import validate, ValidationError
from typing import Dict, Any, Union
import sys

from dotenv import load_dotenv
from pathlib import Path
from gemini_logger import log_gemini_pdf_call

# Load .keys
load_dotenv(Path('.keys'))


# --- Configuration ---
# Get your API key from environment variables (recommended)
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    raise ValueError("GOOGLE_API_KEY environment variable not set. Please set it before running the script.")

genai.configure(api_key=GOOGLE_API_KEY)

# Gemini Model to use
GEMINI_MODEL = "gemini-2.5-flash-preview-05-20" # This model supports multimodal input (like PDF)

# --- JSON Schema Definition (reused from previous answer) ---
def define_json_schema() -> Dict[str, Any]:
    """
    Defines the standard JSON schema for the auto insurance declarations page.
    This schema ensures normalization across different PDFs.
    """
    schema = {
        "type": "object",
        "properties": {
            "document_type": {"type": "string", "enum": ["Auto Insurance Declarations Page"]},
            "policy_summary": {
                "type": "object",
                "properties": {
                    "policy_number": {"type": "string"},
                    "underwritten_by": {"type": "string"},
                    "issue_date": {"type": "string", "format": "date"},
                    "policy_period": {
                        "type": "object",
                        "properties": {
                            "start_date": {"type": "string", "format": "date"},
                            "end_date": {"type": "string", "format": "date"}
                        },
                        "required": ["start_date", "end_date"]
                    },
                    "policy_effective_time": {"type": "string"},
                    "page_info": {"type": "string"},
                    "policy_forms": {"type": "array", "items": {"type": "string"}},
                    "total_6_month_policy_premium": {"type": "string", "pattern": "^\\$[0-9,]+\\.?[0-9]{0,2}$"},
                    "premium_discounts": {"type": "array", "items": {"type": "string"}}
                },
                "required": ["policy_number", "underwritten_by", "issue_date", "policy_period", "total_6_month_policy_premium"]
            },
            "insurance_agent_info": {
                "type": "object",
                "properties": {
                    "agent_name": {"type": "string"},
                    "agent_number": {"type": "string"}
                },
                "required": ["agent_name", "agent_number"]
            },
            "named_insured": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "address": {"type": "string"}
                },
                "required": ["name", "address"]
            },
            "drivers_and_household_residents": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "additional_information": {"type": "string", "nullable": True}
                    },
                    "required": ["name"]
                }
            },
            "vehicle_details": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "year": {"type": "string"},
                        "make": {"type": "string"},
                        "model": {"type": "string"},
                        "vin": {"type": "string"},
                        "garaging_zip_code": {"type": "string"},
                        "primary_use": {"type": "string"},
                        "annual_miles": {"type": "string"},
                        "length_of_ownership": {"type": "string"},
                        "vehicle_history_impact": {"type": "string", "nullable": True},
                        "features": {"type": "array", "items": {"type": "string"}, "nullable": True},
                        "outline_of_coverage": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "coverage": {"type": "string"},
                                    "limits": {"type": "string", "nullable": True},
                                    "deductible": {"type": "string", "nullable": True},
                                    "premium": {"type": "string", "pattern": "^\\$?[0-9,]+\\.?[0-9]{0,2}$", "nullable": True}
                                },
                                "required": ["coverage"]
                            }
                        }
                    },
                    "required": ["year", "make", "model", "vin"]
                }
            },
            "customer_service_info": {
                "type": "object",
                "properties": {
                    "online_service": {"type": "string", "nullable": True},
                    "phone_number": {"type": "string", "nullable": True},
                    "phone_service_hours": {"type": "string", "nullable": True}
                }
            },
            "identification_card": {
                "type": "object",
                "properties": {
                    "card_type": {"type": "string"},
                    "customer_status": {"type": "string", "nullable": True},
                    "valued_customer_since": {"type": "string", "nullable": True},
                    "insurer": {"type": "string"},
                    "policy_number": {"type": "string"},
                    "effective_date": {"type": "string", "format": "date"},
                    "expiration_date": {"type": "string", "format": "date"},
                    "named_insureds": {"type": "array", "items": {"type": "string"}},
                    "vehicle_details_id_card": {
                        "type": "object",
                        "properties": {
                            "year": {"type": "string"},
                            "make": {"type": "string"},
                            "model": {"type": "string"},
                            "vin": {"type": "string"}
                        },
                        "required": ["year", "make", "model", "vin"]
                    },
                    "naic_number": {"type": "string", "nullable": True},
                    "validity_note": {"type": "string", "nullable": True},
                    "coverage_types_checked": {"type": "array", "items": {"type": "string"}, "nullable": True},
                    "coverage_details_note": {"type": "string", "nullable": True},
                    "misrepresentation_warning": {"type": "string", "nullable": True},
                    "accident_instructions": {"type": "array", "items": {"type": "string"}, "nullable": True},
                    "report_claim_info": {
                        "type": "object",
                        "properties": {
                            "phone_number": {"type": "string", "nullable": True},
                            "website": {"type": "string", "nullable": True}
                        }
                    },
                    "card_retention_instruction": {"type": "string", "nullable": True}
                },
                "required": ["card_type", "insurer", "policy_number", "effective_date", "expiration_date", "named_insureds", "vehicle_details_id_card"]
            }
        },
        "required": ["document_type", "policy_summary", "named_insured", "drivers_and_household_residents", "vehicle_details"]
    }
    return schema

# --- Gemini API Call with PDF Upload ---
def call_gemini_api_with_pdf(pdf_file_path: str, json_schema: Dict[str, Any]) -> Union[Dict[str, Any], None]:
    """
    Uploads a PDF file to Gemini and instructs the model to extract structured data,
    conforming to the provided JSON schema.
    """
    model = genai.GenerativeModel(GEMINI_MODEL)
    file_data = None
    try:
        # Upload the PDF file to Gemini's ephemeral storage
        print(f"Uploading PDF: {pdf_file_path}...")
        file_data = genai.upload_file(path=pdf_file_path, display_name=os.path.basename(pdf_file_path))
        print(f"File uploaded: {file_data.uri}")

        # Construct the prompt with the file data
        prompt_text = f"""
        You are an expert at extracting structured information from auto insurance declarations pages.
        Analyze the provided PDF document and extract all relevant details, including policy details,
        insured parties, vehicle information, coverage outlines, and identification card details.

        The output MUST be a JSON object that strictly adheres to the following JSON schema.
        Do not include any text outside of the JSON object.
        If a field is not found in the PDF, omit it. If that field is marked as "required" in the schema, use the value "None".
        For monetary values, include the currency symbol (e.g., "$1,755.00").

        Special note on Agent Name and Agent number fields below, the agent number might appear next to the agent name:
        <example>
          Input string: ESTRELLA INSURANCE #104

          Output should contain:
             agent_name: ESTRELLA INSURANCE
             agent_number: 104
        </example>

        JSON Schema:
        {json.dumps(json_schema, indent=2)}
        """

        # Pass both the file_data (the PDF) and the text prompt as parts of the content
        response = model.generate_content(
            [file_data, {"text": prompt_text}],
            generation_config=genai.types.GenerationConfig(
                response_mime_type="application/json", # Crucial for direct JSON output
                temperature=0.1 # Lower temperature for more deterministic output
            )
        )

        # The response.text will directly contain the JSON string
        json_output = json.loads(response.text)
        
        # Log successful API call
        log_gemini_pdf_call(pdf_file_path, json_output, success=True)
        
        return json_output
    except FileNotFoundError as e:
        error_msg = f"PDF file not found at {pdf_file_path}"
        print(f"Error: {error_msg}")
        log_gemini_pdf_call(pdf_file_path, {}, success=False, error_message=error_msg)
        return None
    except json.JSONDecodeError as e:
        error_msg = f"Error decoding JSON from Gemini response: {e}"
        print(error_msg)
        print(f"Gemini raw response text (start): {response.text[:500]}...") # Print part of the raw response for debugging
        log_gemini_pdf_call(pdf_file_path, {}, success=False, error_message=error_msg)
        return None
    except Exception as e:
        error_msg = f"Error during Gemini API call or file upload: {e}"
        print(error_msg)
        log_gemini_pdf_call(pdf_file_path, {}, success=False, error_message=error_msg)
        return None
    finally:
        # Clean up: Delete the uploaded file from Gemini's storage
        if file_data:
            try:
                genai.delete_file(file_data.name)
                print(f"Cleaned up uploaded file: {file_data.name}")
            except Exception as e:
                print(f"Error deleting file {file_data.name}: {e}")


# --- JSON Validation (reused from previous answer) ---
def validate_json_output(json_data: Dict[str, Any], schema: Dict[str, Any]) -> bool:
    """
    Validates the generated JSON data against the provided schema.
    """
    try:
        validate(instance=json_data, schema=schema)
        print("JSON output successfully validated against the schema.")
        return True
    except ValidationError as e:
        print(f"JSON validation error: {e.message}")
        print(f"Path: {' -> '.join(map(str, e.path))}")
        print(f"Schema path: {' -> '.join(map(str, e.schema_path))}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during JSON validation: {e}")
        return False

# --- Main Processing Function ---
def process_pdf_to_json(pdf_path: str, output_json_path: str = "output.json") -> None:
    """
    Main function to process the PDF, upload to Gemini, and save/validate JSON.
    """
    print(f"Attempting to process PDF: {pdf_path}")

    # 1. Define schema
    schema = define_json_schema()

    # 2. Call Gemini API with PDF
    print("Calling Gemini API with PDF upload...")
    generated_json = call_gemini_api_with_pdf(pdf_path, schema)

    if generated_json:
        # 3. Validate JSON
        if validate_json_output(generated_json, schema):
            # 4. Save JSON
            with open(output_json_path, 'w', encoding='utf-8') as f:
                json.dump(generated_json, f, indent=2, ensure_ascii=False)
            print(f"Successfully generated and saved validated JSON to {output_json_path}")
        else:
            print("Generated JSON did not pass schema validation.")
            # Optionally save the invalid JSON for debugging
            with open("invalid_output.json", 'w', encoding='utf-8') as f:
                json.dump(generated_json, f, indent=2, ensure_ascii=False)
            print("Invalid JSON saved to invalid_output.json for inspection.")
    else:
        print("Failed to generate JSON from Gemini API.")

# --- Example Usage ---
if __name__ == "__main__":
    # IMPORTANT: Replace 'path/to/your/document.pdf' with the actual path to your PDF file.
    # For this example, I'll assume the PDF is named 'insurance_declarations.pdf'
    # and is in the same directory as this script. You MUST provide a real PDF.
    pdf_file_path = "insurance_declarations.pdf" # <--- CHANGE THIS TO YOUR ACTUAL PDF FILE PATH
    pdf_file_path = sys.argv[1]

    if os.path.exists(pdf_file_path):
        process_pdf_to_json(pdf_file_path, "extracted_insurance_data_direct.json")
    else:
        print(f"\nError: PDF file not found at '{pdf_file_path}'.")
        print("Please ensure your PDF file exists at this path or update the 'pdf_file_path' variable.")
        print("You can download the sample PDF from the original problem description and save it as 'insurance_declarations.pdf'.")
