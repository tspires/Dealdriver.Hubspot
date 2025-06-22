"""Setup script for Dealdriver HubSpot integration."""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="dealdriver-hubspot",
    version="1.0.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="HubSpot CRM enrichment through web scraping and AI analysis",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/dealdriver-hubspot",
    packages=find_packages(exclude=["tests", "tests.*", "scripts", "scripts.*"]),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "dealdriver-hubspot=src.main:main",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.ini", "*.yaml", "*.yml", "*.json"],
    },
)