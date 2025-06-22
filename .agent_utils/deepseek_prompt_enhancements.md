# DeepSeek Prompt Enhancement Guide

## Current State Analysis

### Common Library Strengths
- Structured JSON output with strict schema
- HubSpot CRM field alignment
- System prompt for JSON-only responses
- NAICS validation
- Email context integration
- Content truncation at sentence boundaries

### Project Implementation Gaps
- Not using the sophisticated `analyze_business_website()` method
- Basic prompts without structure
- No validation or error handling
- Missing confidence calibration

## Recommended Enhancements

### 1. **Immediate: Switch to Common Library Method**

```python
# Instead of custom prompts, use:
result = self.client.analyze_business_website(
    website_content=content,
    domain=domain,
    extracted_emails=emails  # Pass scraped emails for context
)
```

### 2. **Enhanced Prompt Engineering Techniques**

#### A. Chain-of-Thought (CoT) Prompting
```python
COT_BUSINESS_PROMPT = """
Let's analyze this business step-by-step:

Step 1: Identify the primary business activity from the content
Step 2: Determine the industry category
Step 3: Find the most specific NAICS code
Step 4: List all mentioned products/services
Step 5: Identify the target customer base
Step 6: Extract competitive advantages
Step 7: Note any certifications or credibility indicators

Based on this analysis, provide the structured JSON output:
{json_schema}
"""
```

#### B. Few-Shot Learning
Add 2-3 examples before the actual content:
```python
EXAMPLES = [
    {
        "input": "ABC Roofing - Commercial & Residential roofing since 1992...",
        "output": {
            "naics_code": "238160",
            "business_type_description": "Roofing Contractor",
            # ... complete example
        }
    }
]
```

#### C. Role-Based Prompting
```python
EXPERT_SYSTEM_PROMPT = """
You are a team of experts:
1. NAICS Classification Specialist (10 years experience)
2. Business Intelligence Analyst (Fortune 500 background)
3. Market Research Expert (Industry reports author)

Work together to provide the most accurate analysis.
"""
```

#### D. Structured Reasoning
```python
STRUCTURED_ANALYSIS_PROMPT = """
Analyze using this framework:

1. OBSERVABLE FACTS
   - Direct statements from the website
   - Specific services/products mentioned
   - Location information

2. REASONABLE INFERENCES
   - Industry based on services
   - Company size from context clues
   - Target market from language used

3. CONFIDENCE ASSESSMENT
   - High confidence: Directly stated facts
   - Medium confidence: Strong context clues
   - Low confidence: Educated guesses

Output your analysis in the required JSON format.
"""
```

### 3. **Dynamic Prompt Selection**

```python
def select_prompt_strategy(content_length: int, content_quality: str) -> str:
    """Select optimal prompt based on content characteristics."""
    
    if content_length < 500:
        # Use focused prompt for limited content
        return MINIMAL_CONTENT_PROMPT
    elif content_length > 5000:
        # Use summary-first approach
        return LARGE_CONTENT_PROMPT
    elif "About Us" in content and "Services" in content:
        # Use detailed analysis
        return DETAILED_ANALYSIS_PROMPT
    else:
        # Use chain-of-thought for unclear content
        return COT_ANALYSIS_PROMPT
```

### 4. **Enhanced Lead Scoring Prompt**

```python
ADVANCED_LEAD_SCORING = """
Score this lead using multiple dimensions:

1. ICP FIT SCORE (Ideal Customer Profile)
   - Company size alignment
   - Industry match
   - Geographic fit
   
2. PERSONA SCORE
   - Job title indicators
   - Department alignment
   - Seniority level
   
3. INTENT SIGNALS
   - Email pattern (personal vs generic)
   - Page visited context
   - Time of engagement
   
4. BUDGET AUTHORITY
   - Title suggests budget control
   - Department typically has budget
   
Provide scores for each dimension and overall recommendation.
"""
```

### 5. **Confidence Calibration**

```python
CONFIDENCE_RUBRIC = """
Assign confidence scores based on:

9-10: All key information explicitly stated
7-8: Most information clear with minor inference
5-6: Mix of stated facts and reasonable inference  
3-4: Mostly inference from limited data
1-2: Minimal information, high uncertainty

Be conservative - when in doubt, lower confidence.
"""
```

### 6. **Error Recovery Prompts**

```python
FALLBACK_PROMPTS = [
    "Focus only on clearly stated facts from the website",
    "Provide minimal analysis with only high-confidence data",
    "Extract just company name, industry, and one service"
]

def analyze_with_fallback(content: str, attempts: int = 3):
    """Try progressively simpler prompts on failure."""
    for i in range(attempts):
        try:
            if i == 0:
                return analyze_with_main_prompt(content)
            else:
                return analyze_with_fallback_prompt(content, FALLBACK_PROMPTS[i-1])
        except Exception as e:
            logger.warning(f"Attempt {i+1} failed: {e}")
    return minimal_analysis(content)
```

### 7. **Context Enhancement**

```python
def enhance_content_context(content: str, domain: str, emails: List[str]) -> str:
    """Add structured context to improve analysis."""
    
    enhanced = f"""
    DOMAIN: {domain}
    EMAIL PATTERNS: {analyze_email_patterns(emails)}
    CONTENT LENGTH: {len(content)} characters
    
    === WEBSITE CONTENT ===
    {content}
    
    === EXTRACTED EMAILS ===
    {', '.join(emails[:5])}
    """
    return enhanced
```

### 8. **Prompt Testing Framework**

```python
class PromptTester:
    """A/B test different prompts for optimization."""
    
    def __init__(self):
        self.prompt_versions = {
            'v1': BUSINESS_ANALYSIS_PROMPT,
            'v2': COT_BUSINESS_PROMPT,
            'v3': FEW_SHOT_PROMPT
        }
        self.results = defaultdict(list)
    
    def test_prompt(self, content: str, ground_truth: dict) -> dict:
        """Test all prompt versions and compare results."""
        for version, prompt in self.prompt_versions.items():
            result = self.analyze_with_prompt(content, prompt)
            score = self.calculate_accuracy(result, ground_truth)
            self.results[version].append(score)
        return self.get_best_prompt()
```

## Implementation Priority

1. **Immediate** (1 day):
   - Switch to using `analyze_business_website()` from common library
   - Add email context to all analyses

2. **Short-term** (1 week):
   - Implement few-shot examples
   - Add confidence calibration
   - Create fallback strategies

3. **Medium-term** (2-3 weeks):
   - Build prompt testing framework
   - Implement dynamic prompt selection
   - Add chain-of-thought for complex cases

4. **Long-term** (1 month+):
   - A/B test prompt variations
   - Build prompt performance dashboard
   - Create prompt template library

## Performance Metrics

Track these metrics to measure improvement:
- JSON parse success rate
- Confidence score accuracy
- NAICS code validity
- Field completion rate
- Processing time
- Token usage efficiency

## Example Implementation

```python
# In analyzer.py
def analyze_company_enhanced(self, content: str, domain: str, emails: List[str]) -> Optional[CompanyAnalysis]:
    """Enhanced company analysis using all improvements."""
    
    # 1. Use the sophisticated common library method
    try:
        result = self.client.analyze_business_website(
            website_content=content,
            domain=domain,
            extracted_emails=emails
        )
        
        # 2. Validate and enhance if needed
        if result['confidence_score'] < 5:
            # Try chain-of-thought approach
            result = self.analyze_with_cot(content, domain)
        
        # 3. Map to our model
        return self._map_to_company_analysis(result)
        
    except Exception as e:
        logger.error("Primary analysis failed: %s", e)
        # 4. Fallback to simpler analysis
        return self.simple_analysis_fallback(content, domain)
```

The DeepSeek implementation in the common library is already quite sophisticated. The main opportunity is ensuring your project properly leverages it and adds these additional prompt engineering techniques for even better results.