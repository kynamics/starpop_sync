import pandas as pd
from typing import Optional, Dict


class StarAgentMatcher:
    """ This class is used to match the agent code to the DBA name.
    It loads the "Star Agents List.xlsx" file and creates two hashmaps:
    - match_to_dba: Maps "Match" column values to DBAName
    - agent_code_to_dba: Maps "AgentCode" column values to DBAName

    The class provides the following methods:
    - get_dba_by_match(match_value) - Returns DBAName for a given Match value
    - get_dba_by_agent_code(agent_code) - Returns DBAName for a given AgentCode value
    - get_all_matches() - Returns all Match -> DBAName mappings
    - get_all_agent_codes() - Returns all AgentCode -> DBAName mappings

  Usage example:
    matcher = StarAgentMatcher()
    dba_name = matcher.get_dba_by_match("some_match_value")
    dba_name = matcher.get_dba_by_agent_code("some_agent_code")
    all_matches = matcher.get_all_matches()
    all_agent_codes = matcher.get_all_agent_codes()

    print(all_matches)
    print(all_agent_codes)


    """
    def __init__(self, excel_file_path: str = "Star Agents List.xlsx"):
        self.excel_file_path = excel_file_path
        self.match_to_dba: Dict[str, str] = {}
        self.agent_code_to_dba: Dict[str, str] = {}
        self._load_data()
    
    def _load_data(self):
        """Load the Excel file and create the hashmaps."""
        try:
            # Load the Excel file
            df = pd.read_excel(self.excel_file_path)
            
            # Create hashmaps
            for _, row in df.iterrows():
                dba_name = row.get('DBAName')
                match_value = row.get('Match')
                agent_code = row.get('AgentCode')
                
                # Map Match column to DBAName
                if pd.notna(match_value) and pd.notna(dba_name):
                    self.match_to_dba[str(match_value)] = str(dba_name)
                
                # Map AgentCode column to DBAName
                if pd.notna(agent_code) and pd.notna(dba_name):
                    self.agent_code_to_dba[str(agent_code)] = str(dba_name)
                    
        except Exception as e:
            print(f"Error loading Excel file: {e}")
            raise
    
    def get_dba_by_match(self, match_value: str) -> Optional[str]:
        """Return DBAName given a Match value."""
        return self.match_to_dba.get(str(match_value))
    
    def get_dba_by_agent_code(self, agent_code: str) -> Optional[str]:
        """Return DBAName given an AgentCode value."""
        return self.agent_code_to_dba.get(str(agent_code))
    
    def get_all_matches(self) -> Dict[str, str]:
        """Return the complete Match -> DBAName mapping."""
        return self.match_to_dba.copy()
    
    def get_all_agent_codes(self) -> Dict[str, str]:
        """Return the complete AgentCode -> DBAName mapping."""
        return self.agent_code_to_dba.copy()
    
    def get_dba_by_match_or_agent_code(self, code:str) -> Optional[str]:
        """Return DBAName given a Match or AgentCode value."""
        dba = self.match_to_dba.get(str(code), None)
        if dba is None:
            dba = self.agent_code_to_dba.get(str(code), None)
        return dba

    def compute_match(self, code_a: str, code_b: str) -> bool:
        """Compute if the two codes match."""
        dba_a = self.get_dba_by_match_or_agent_code(code_a)
        dba_b = self.get_dba_by_match_or_agent_code(code_b)
        if dba_a is None or dba_b is None:
            return False
        return dba_a == dba_b