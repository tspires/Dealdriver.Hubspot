"""Enhanced AI analysis service using improved DeepSeek prompts."""

import json
import logging
from typing import Dict, Optional, List, Any
from src.models.enrichment import CompanyAnalysis, LeadAnalysis

logger = logging.getLogger(__name__)


class EnhancedAIAnalyzer:
    """Enhanced analyzer using DeepSeek's analyze_business_website method."""
    
    def __init__(self):
        """Initialize analyzer."""
        logger.info("Initializing EnhancedAIAnalyzer")
        try:
            import sys
            from pathlib import Path
            common_path = Path(__file__).parent.parent.parent.parent / "common"
            sys.path.insert(0, str(common_path))
            
            from clients.deepseek import DeepSeekClient
            self.client = DeepSeekClient()
            logger.info("DeepSeekClient initialized successfully")
        except Exception as e:
            logger.error("Failed to import DeepSeekClient: %s", e)
            self.client = None
    
    def analyze_company(self, content: str, domain: str = None, emails: List[str] = None) -> Optional[CompanyAnalysis]:
        """Analyze content using DeepSeek's sophisticated business analysis."""
        if not self.client:
            return None
            
        try:
            # Use the sophisticated analyze_business_website method
            result = self.client.analyze_business_website(
                website_content=content,
                domain=domain,
                extracted_emails=emails
            )
            
            # Map the result to our CompanyAnalysis model
            return self._map_to_company_analysis(result)
            
        except Exception as e:
            logger.error("Failed to analyze company: %s", e)
            return None
    
    def _map_to_company_analysis(self, result: Dict[str, Any]) -> CompanyAnalysis:
        """Map DeepSeek result to CompanyAnalysis model."""
        company = result.get('company', {})
        bi = result.get('business_intelligence', {})
        
        return CompanyAnalysis(
            business_type_description=bi.get('business_type_description', ''),
            company_summary=company.get('description', ''),
            industry=company.get('industry', ''),
            naics_code=bi.get('naics_code', ''),
            company_owner=None,  # Not in DeepSeek response
            city=company.get('city'),
            state_region=company.get('state'),
            postal_code=company.get('zip'),
            country=company.get('country'),
            number_of_employees=company.get('numberofemployees'),
            annual_revenue=company.get('annualrevenue'),
            timezone=None,  # Not in DeepSeek response
            target_market=bi.get('target_market', ''),
            primary_products_services=bi.get('primary_products_services', []),
            value_propositions=bi.get('value_propositions', []),
            competitive_advantages=bi.get('competitive_advantages', []),
            technologies_used=bi.get('technologies_used', []),
            certifications_awards=bi.get('certifications_awards', []),
            pain_points_addressed=bi.get('pain_points_addressed', []),
            confidence_score=result.get('confidence_score', 5) / 10.0  # Convert to 0-1 scale
        )


# Enhanced prompt templates for additional features
ENHANCED_LEAD_PROMPT = """You are an expert B2B lead scoring specialist with deep knowledge of buyer personas and sales qualification.

CONTEXT:
- Website Domain: {domain}
- Email Found: {email}
- Company Context: {company_context}

WEBSITE CONTENT:
{content}

ANALYSIS REQUIREMENTS:
1. First, determine the email pattern (personal vs role-based vs generic)
2. Analyze the website to understand the company's target customer
3. Infer the likely role/persona based on email pattern and company context
4. Score the lead based on alignment with typical B2B buying patterns

OUTPUT FORMAT (strict JSON):
{{
    "email_analysis": {{
        "pattern": "<personal|role_based|generic|executive>",
        "likely_department": "<sales|marketing|engineering|executive|operations|other>",
        "seniority_indicators": ["<indicator1>", "<indicator2>"]
    }},
    "buyer_persona": "<Technical Decision Maker|Business Executive|End User|Influencer|Economic Buyer|Unknown>",
    "lead_score_adjustment": <-10 to +10>,
    "qualification_factors": {{
        "company_fit": <1-10>,
        "role_fit": <1-10>,
        "timing_signals": <1-10>,
        "engagement_potential": <1-10>
    }},
    "reasoning": "<detailed explanation of scoring>",
    "confidence": <0.0 to 1.0>
}}

SCORING GUIDELINES:
- Executive emails (ceo@, president@): +7 to +10
- Department heads (vp@, director@): +5 to +7
- Technical roles with budget authority: +3 to +5
- Individual contributors: 0 to +3
- Generic emails (info@, contact@): -5 to 0
- Suspicious patterns: -10 to -5
"""

# Chain-of-thought prompt for complex analysis
COT_ANALYSIS_PROMPT = """Let's analyze this step-by-step:

Step 1: What is the primary business activity?
Step 2: What industry does this align with?
Step 3: What's the appropriate NAICS classification?
Step 4: What products/services are explicitly mentioned?
Step 5: What's implied but not stated?
Step 6: Who are their likely customers?
Step 7: What problems do they solve?

Now synthesize into a structured analysis..."""

# Few-shot example prompt
FEW_SHOT_PROMPT = """Here are examples of high-quality analyses:

Example 1:
Input: "Acme Plumbing provides 24/7 emergency plumbing services to homeowners in Chicago. Licensed and insured since 1985."
Output: {
    "business_type_description": "Residential Plumbing Contractor",
    "naics_code": "238220",
    "target_market": "Homeowners in Chicago metro area with plumbing emergencies",
    "primary_products_services": ["Emergency plumbing repairs", "24/7 service", "Residential plumbing"],
    "value_propositions": ["24/7 availability", "35+ years experience", "Licensed and insured"],
    "confidence_score": 9
}

Example 2:
Input: "TechFlow Solutions builds custom CRM integrations for Salesforce. We help enterprises streamline their sales processes."
Output: {
    "business_type_description": "CRM Integration Consultancy",
    "naics_code": "541511",
    "target_market": "Enterprise companies using Salesforce CRM",
    "primary_products_services": ["Salesforce integrations", "CRM customization", "Sales process consulting"],
    "value_propositions": ["Custom solutions", "Enterprise expertise", "Process optimization"],
    "confidence_score": 8
}

Now analyze the following:
{content}"""