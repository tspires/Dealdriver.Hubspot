"""AI analysis service."""

import json
import logging
from typing import Dict, Optional, List

from src.models.enrichment import CompanyAnalysis, LeadAnalysis


logger = logging.getLogger(__name__)


class AIAnalyzer:
    """Service for AI-powered content analysis."""
    
    def __init__(self):
        """Initialize analyzer."""
        logger.info("Initializing AIAnalyzer")
        try:
            import sys
            from pathlib import Path
            # Add common library to path
            common_path = Path(__file__).parent.parent.parent.parent / "common"
            logger.debug("Adding common path to sys.path: %s", common_path)
            sys.path.insert(0, str(common_path))
            
            logger.debug("Importing DeepSeekClient from common library")
            from clients.deepseek import DeepSeekClient
            self.client = DeepSeekClient()
            logger.info("DeepSeekClient initialized successfully")
        except Exception as e:
            logger.error("Failed to import DeepSeekClient: %s", e)
            logger.debug("DeepSeekClient initialization error", exc_info=True)
            self.client = None
            logger.critical("AIAnalyzer initialization failed - AI analysis will not work")
    
    def analyze_lead(self, content: str, lead_info: Dict[str, any]) -> Optional[LeadAnalysis]:
        """Analyze content for lead scoring."""
        logger.info("Starting lead analysis for %s", lead_info.get('email', 'unknown'))
        logger.debug("Lead info: %s", lead_info)
        logger.debug("Content length: %d chars", len(content) if content else 0)
        
        if not self.client:
            logger.error("DeepSeekClient not available for lead analysis")
            return None
        
        prompt = f"""
        Analyze the following website content to determine the buyer persona and lead score adjustment.
        
        Lead Information:
        - Name: {lead_info.get('firstname', '')} {lead_info.get('lastname', '')}
        - Company: {lead_info.get('company', '')}
        - Email: {lead_info.get('email', '')}
        - Email Domain: {lead_info.get('email', '').split('@')[-1] if '@' in lead_info.get('email', '') else 'Unknown'}
        
        Website Content (may be incomplete due to JavaScript rendering):
        {content[:4000]}
        
        Based on available information, make your best assessment. If content is limited, use:
        - Email domain patterns (e.g., corporate vs personal email)
        - Any visible company information
        - Technical indicators in the HTML/JS if present
        
        Provide a JSON response with:
        {{
            "buyer_persona": "One of: Technical Decision Maker, Business Executive, End User, Influencer, Unknown",
            "lead_score_adjustment": -10 to +10 based on fit (use 0 if uncertain),
            "confidence": 0.0 to 1.0 (lower confidence if limited data),
            "reasoning": "Brief explanation of your assessment"
        }}
        """
        
        try:
            logger.debug("Sending lead analysis request to DeepSeek API")
            logger.debug("Prompt length: %d chars", len(prompt))
            
            response = self.client.chat_completion(
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=500
            )
            logger.debug("Received response from DeepSeek API")
            # Extract the content from the response
            content = response['choices'][0]['message']['content']
            logger.debug("DeepSeek lead analysis response length: %d chars", len(content))
            logger.debug("Raw response: %s", content[:200] + "..." if len(content) > 200 else content)
            
            # Try to extract JSON from the response
            # Sometimes the model includes extra text
            import re
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                json_str = json_match.group()
                data = json.loads(json_str)
            else:
                data = json.loads(content)
            
            lead_analysis = LeadAnalysis(
                buyer_persona=data["buyer_persona"],
                lead_score_adjustment=data["lead_score_adjustment"],
                confidence=data["confidence"],
                reasoning=data["reasoning"]
            )
            
            logger.info("Lead analysis completed - Persona: %s, Score: %+d, Confidence: %.2f",
                       lead_analysis.buyer_persona, 
                       lead_analysis.lead_score_adjustment,
                       lead_analysis.confidence)
            logger.debug("Reasoning: %s", lead_analysis.reasoning)
            
            return lead_analysis
        except json.JSONDecodeError as e:
            logger.error("Failed to parse JSON response from DeepSeek: %s", e)
            logger.debug("Invalid JSON content: %s", content)
            return None
        except Exception as e:
            logger.error("Failed to analyze lead: %s", e)
            logger.debug("Lead analysis error details", exc_info=True)
            return None
    
    def analyze_company(self, content: str, domain: str = None, emails: List[str] = None) -> Optional[CompanyAnalysis]:
        """Analyze content for company enrichment using sophisticated DeepSeek analysis."""
        logger.info("Starting company analysis for domain: %s", domain or "unknown")
        logger.debug("Content length: %d chars", len(content) if content else 0)
        logger.debug("Extracted emails: %s", emails if emails else "None")
        
        if not self.client:
            logger.error("DeepSeekClient not available for company analysis")
            return None
        
        if not content or len(content) < 50:
            logger.warning("Content too short for meaningful analysis: %d chars", len(content) if content else 0)
            return None
        
        try:
            # Use the sophisticated analyze_business_website method from common library
            logger.debug("Using DeepSeekClient.analyze_business_website method")
            logger.debug("Sending request with domain=%s, emails=%s", domain, emails)
            
            result = self.client.analyze_business_website(
                website_content=content,
                domain=domain,
                extracted_emails=emails
            )
            
            logger.debug("Received structured response from DeepSeek")
            
            # Check for errors in the response
            if result.get('_error'):
                logger.error("DeepSeek analysis error: %s", result.get('_error'))
                return None
            
            if result.get('_parse_error'):
                logger.error("JSON parsing error in DeepSeek response: %s", result.get('_parse_error_details'))
                logger.debug("Raw response: %s", result.get('_raw_response', '')[:500])
                return None
            
            # Extract data from the structured response
            company_data = result.get('company', {})
            bi_data = result.get('business_intelligence', {})
            
            logger.info("Successfully parsed AI response")
            logger.debug("Company data: %s", company_data.get('name', 'Unknown'))
            logger.debug("Business type: %s", bi_data.get('business_type_description', 'Unknown'))
            logger.debug("NAICS: %s", bi_data.get('naics_code', 'Unknown'))
            
            # Map to our CompanyAnalysis model
            company_analysis = CompanyAnalysis(
                business_type_description=bi_data.get('business_type_description', ''),
                company_summary=company_data.get('description', ''),
                industry=company_data.get('industry', ''),
                naics_code=bi_data.get('naics_code', ''),
                company_owner=None,  # Not provided by analyze_business_website
                city=company_data.get('city'),
                state_region=company_data.get('state'),
                postal_code=company_data.get('zip'),
                country=company_data.get('country'),
                number_of_employees=company_data.get('numberofemployees'),
                annual_revenue=company_data.get('annualrevenue'),
                timezone=None,  # Not provided by analyze_business_website
                target_market=bi_data.get('target_market', ''),
                primary_products_services=bi_data.get('primary_products_services', []),
                value_propositions=bi_data.get('value_propositions', []),
                competitive_advantages=bi_data.get('competitive_advantages', []),
                technologies_used=bi_data.get('technologies_used', []),
                certifications_awards=bi_data.get('certifications_awards', []),
                pain_points_addressed=bi_data.get('pain_points_addressed', []),
                confidence_score=result.get('confidence_score', 5) / 10.0  # Convert 1-10 to 0-1 scale
            )
            
            logger.info("Company analysis completed - Industry: %s, NAICS: %s, Confidence: %.2f",
                       company_analysis.industry,
                       company_analysis.naics_code,
                       company_analysis.confidence_score)
            logger.debug("Products/Services: %d, Technologies: %d, Value Props: %d",
                        len(company_analysis.primary_products_services),
                        len(company_analysis.technologies_used),
                        len(company_analysis.value_propositions))
            
            # Log low confidence scores for monitoring
            if company_analysis.confidence_score < 0.5:
                logger.warning("Low confidence score (%.2f) for domain %s", 
                             company_analysis.confidence_score, domain)
            
            return company_analysis
            
        except ValueError as ve:
            logger.error("Validation error during company analysis: %s", ve)
            return None
        except Exception as e:
            logger.error("Failed to analyze company: %s", e)
            logger.debug("Company analysis error details", exc_info=True)
            return None