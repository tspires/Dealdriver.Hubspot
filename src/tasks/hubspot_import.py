"""Luigi tasks for HubSpot bulk import."""

import json
import csv
import luigi
import logging
import time
import sys
from pathlib import Path
from typing import Dict, List, Optional
import os

# Add common library to path
common_path = Path(__file__).parent.parent.parent.parent / "common"
sys.path.insert(0, str(common_path))

logger = logging.getLogger(__name__)


class HubSpotBulkImportTask(luigi.Task):
    """Task to perform bulk import of CSV data into HubSpot."""

    csv_file = luigi.Parameter()
    object_type = luigi.Parameter()  # 'companies' or 'contacts'
    hubspot_token = luigi.Parameter(default="", significant=False)

    def requires(self):
        """No dependencies - runs after CSV is created."""
        return []

    def output(self):
        """Output marker file to track completion."""
        csv_path = Path(self.csv_file)
        import_marker = csv_path.parent / f".imported_{csv_path.stem}.json"
        return luigi.LocalTarget(str(import_marker))

    def run(self):
        """Execute the bulk import to HubSpot."""
        logger.info(
            "Starting HubSpot bulk import for %s from %s",
            self.object_type, self.csv_file
        )

        # Get HubSpot token
        token = self.hubspot_token or os.environ.get("HUBSPOT_TOKEN")
        if not token:
            logger.error("No HubSpot token provided. Skipping import.")
            # Create empty marker file to mark as complete
            with open(self.output().path, "w", encoding="utf-8") as f:
                json.dump({"status": "skipped", "reason": "no_token"}, f)
            return

        try:
            # Check if we're in test mode
            if os.environ.get("HUBSPOT_IMPORT_TEST_MODE"):
                # In test mode, create a mock client
                logger.info("Running in test mode - using mock HubSpot client")
                client = self._get_test_client()
            else:
                # Import HubSpot client
                from clients.hubspot import HubSpotClient

                # Initialize client
                client = HubSpotClient(access_token=token)

            # Read CSV file
            records = self._read_csv_file()
            if not records:
                logger.info("No records to import from %s", self.csv_file)
                with open(self.output().path, "w", encoding="utf-8") as f:
                    json.dump({"status": "completed", "imported": 0}, f)
                return

            # Perform bulk import based on object type
            if self.object_type == "companies":
                results = self._import_companies(client, records)
            elif self.object_type == "contacts":
                results = self._import_contacts(client, records)
            else:
                raise ValueError(f"Unsupported object type: {self.object_type}")

            # Save import results
            with open(self.output().path, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2)

            logger.info(
                "Completed HubSpot import: %s records imported, %s failed",
                results["imported"], results["failed"]
            )

        except Exception as e:
            logger.error("Failed to import to HubSpot: %s", str(e))
            # Save error state
            with open(self.output().path, "w", encoding="utf-8") as f:
                json.dump({"status": "error", "error": str(e)}, f)
            raise

    def _read_csv_file(self) -> List[Dict[str, str]]:
        """Read records from CSV file."""
        records = []
        try:
            with open(self.csv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Skip empty rows
                    if any(row.values()):
                        records.append(row)
            return records
        except Exception as e:
            logger.error("Failed to read CSV file: %s", e)
            return []

    def _import_companies(self, client, records: List[Dict[str, str]]) -> Dict:
        """Import companies to HubSpot."""
        results = {
            "status": "completed",
            "object_type": "companies",
            "total": len(records),
            "imported": 0,
            "updated": 0,
            "failed": 0,
            "errors": []
        }

        for record in records:
            try:
                # Skip unsuccessful enrichments
                if record.get("success", "").lower() != "true":
                    logger.debug(
                        "Skipping unsuccessful enrichment for %s",
                        record.get("domain")
                    )
                    continue

                domain = record.get("domain", "").strip()
                if not domain:
                    continue

                # Prepare company properties
                properties = self._prepare_company_properties(record)

                # Check if company exists
                existing = self._find_company_by_domain(client, domain)

                if existing:
                    # Update existing company
                    client.update_company(
                        company_id=existing["id"],
                        properties=properties
                    )
                    results["updated"] += 1
                    logger.debug("Updated company: %s", domain)
                else:
                    # Create new company
                    client.create_company(properties=properties)
                    results["imported"] += 1
                    logger.debug("Created company: %s", domain)

                # Rate limiting
                time.sleep(0.1)

            except Exception as e:
                results["failed"] += 1
                error_msg = "Failed to import company %s: %s" % (
                    record.get("domain", "unknown"), str(e)
                )
                results["errors"].append(error_msg)
                logger.error(error_msg)

        return results

    def _import_contacts(self, client, records: List[Dict[str, str]]) -> Dict:
        """Import contacts to HubSpot."""
        results = {
            "status": "completed",
            "object_type": "contacts",
            "total": len(records),
            "imported": 0,
            "updated": 0,
            "failed": 0,
            "errors": []
        }

        for record in records:
            try:
                email = record.get("email", "").strip()
                if not email:
                    continue

                # Prepare contact properties
                properties = self._prepare_contact_properties(record)

                # Check if contact exists
                existing = self._find_contact_by_email(client, email)

                if existing:
                    # Update existing contact
                    client.update_contact(
                        contact_id=existing["id"],
                        properties=properties
                    )
                    results["updated"] += 1
                    logger.debug("Updated contact: %s", email)
                else:
                    # Create new contact
                    client.create_contact(properties=properties)
                    results["imported"] += 1
                    logger.debug("Created contact: %s", email)

                # Rate limiting
                time.sleep(0.1)

            except Exception as e:
                results["failed"] += 1
                error_msg = "Failed to import contact %s: %s" % (
                    record.get("email", "unknown"), str(e)
                )
                results["errors"].append(error_msg)
                logger.error(error_msg)

        return results

    def _prepare_company_properties(
        self, record: Dict[str, str]
    ) -> Dict[str, str]:
        """Prepare company properties for HubSpot API."""
        # Map CSV fields to HubSpot properties
        properties = {
            "domain": record.get("domain", ""),
            # Use domain as name if not provided
            "name": record.get("domain", ""),
        }

        # Standard properties
        if record.get("business_type"):
            properties["business_type_description"] = (
                record["business_type"]
            )
        if record.get("naics_code"):
            properties["naics_code"] = record["naics_code"]
        if record.get("target_market"):
            properties["target_market"] = record["target_market"]
        if record.get("confidence_score"):
            properties["confidence_score"] = record["confidence_score"]

        # Multi-value fields (semicolon-separated in CSV)
        if record.get("products_services"):
            properties["primary_products_services"] = (
                record["products_services"]
            )
        if record.get("value_propositions"):
            properties["value_propositions"] = record["value_propositions"]
        if record.get("competitive_advantages"):
            properties["competitive_advantages"] = (
                record["competitive_advantages"]
            )
        if record.get("technologies"):
            properties["technologies_used"] = record["technologies"]
        if record.get("certifications"):
            properties["certifications_awards"] = record["certifications"]
        if record.get("pain_points"):
            properties["pain_points_addressed"] = record["pain_points"]

        # Metadata
        properties["enrichment_status"] = "completed"
        properties["enrichment_date"] = record.get("enriched_at", "")

        return properties

    def _prepare_contact_properties(
        self, record: Dict[str, str]
    ) -> Dict[str, str]:
        """Prepare contact properties for HubSpot API."""
        properties = {
            "email": record.get("email", ""),
        }

        if record.get("first_name"):
            properties["firstname"] = record["first_name"]
        if record.get("last_name"):
            properties["lastname"] = record["last_name"]
        if record.get("company_domain"):
            properties["company"] = record["company_domain"]
        if record.get("buyer_persona"):
            properties["buyer_persona"] = record["buyer_persona"]
        if record.get("lead_score_adjustment"):
            properties["lead_score_adjustment"] = (
                record["lead_score_adjustment"]
            )

        # Metadata
        properties["enrichment_status"] = "completed"
        properties["enrichment_date"] = record.get("enriched_at", "")

        return properties

    def _find_company_by_domain(self, client, domain: str) -> Optional[Dict]:
        """Find existing company by domain."""
        try:
            # Search for company by domain
            results = client.search_companies(
                filter_groups=[{
                    "filters": [{
                        "propertyName": "domain",
                        "operator": "EQ",
                        "value": domain
                    }]
                }],
                properties=["domain"],
                limit=1
            )

            if results and results.get("results"):
                return results["results"][0]
            return None

        except Exception as e:
            logger.debug("Error searching for company %s: %s", domain, e)
            return None

    def _find_contact_by_email(self, client, email: str) -> Optional[Dict]:
        """Find existing contact by email."""
        try:
            # Search for contact by email
            results = client.search_contacts(
                filter_groups=[{
                    "filters": [{
                        "propertyName": "email",
                        "operator": "EQ",
                        "value": email
                    }]
                }],
                properties=["email"],
                limit=1
            )

            if results and results.get("results"):
                return results["results"][0]
            return None

        except Exception as e:
            logger.debug("Error searching for contact %s: %s", email, e)
            return None

    def _get_test_client(self):
        """Get a mock client for testing."""
        from unittest.mock import Mock

        mock_client = Mock()
        mock_client.search_companies = Mock(return_value={"results": []})
        mock_client.search_contacts = Mock(return_value={"results": []})
        mock_client.create_company = Mock(
            side_effect=lambda **kwargs: {
                "id": "test_%s" % kwargs.get("properties", {}).get(
                    "domain", "unknown"
                )
            }
        )
        mock_client.create_contact = Mock(
            side_effect=lambda **kwargs: {
                "id": "test_%s" % kwargs.get("properties", {}).get(
                    "email", "unknown"
                )
            }
        )
        mock_client.update_company = Mock(
            side_effect=lambda **kwargs: {"id": kwargs.get("company_id")}
        )
        mock_client.update_contact = Mock(
            side_effect=lambda **kwargs: {"id": kwargs.get("contact_id")}
        )

        return mock_client


class ImportCompaniesTask(luigi.Task):
    """Wrapper task to import companies after CSV export."""

    company_csv = luigi.Parameter()
    hubspot_token = luigi.Parameter(default="", significant=False)

    def requires(self):
        """Require the CSV file to exist."""
        # Return empty list - CSV creation is handled by pipeline
        return []

    def output(self):
        """Use the bulk import task output."""
        return HubSpotBulkImportTask(
            csv_file=self.company_csv,
            object_type="companies",
            hubspot_token=self.hubspot_token
        ).output()

    def run(self):
        """Run the bulk import task."""
        # Delegate to bulk import task
        task = HubSpotBulkImportTask(
            csv_file=self.company_csv,
            object_type="companies",
            hubspot_token=self.hubspot_token
        )
        luigi.build([task], local_scheduler=True)


class ImportLeadsTask(luigi.Task):
    """Wrapper task to import leads after CSV export."""

    leads_csv = luigi.Parameter()
    hubspot_token = luigi.Parameter(default="", significant=False)

    def requires(self):
        """Require the CSV file to exist."""
        # Return empty list - CSV creation is handled by pipeline
        return []

    def output(self):
        """Use the bulk import task output."""
        return HubSpotBulkImportTask(
            csv_file=self.leads_csv,
            object_type="contacts",
            hubspot_token=self.hubspot_token
        ).output()

    def run(self):
        """Run the bulk import task."""
        # Delegate to bulk import task
        task = HubSpotBulkImportTask(
            csv_file=self.leads_csv,
            object_type="contacts",
            hubspot_token=self.hubspot_token
        )
        luigi.build([task], local_scheduler=True)


class ImportAllTask(luigi.Task):
    """Task to import both companies and leads to HubSpot."""

    company_csv = luigi.Parameter()
    leads_csv = luigi.Parameter()
    hubspot_token = luigi.Parameter(default="", significant=False)

    def requires(self):
        """Import both companies and leads."""
        return [
            ImportCompaniesTask(
                company_csv=self.company_csv,
                hubspot_token=self.hubspot_token
            ),
            ImportLeadsTask(
                leads_csv=self.leads_csv,
                hubspot_token=self.hubspot_token
            )
        ]

    def output(self):
        """Mark as complete when both imports are done."""
        # Create a combined marker file
        company_path = Path(self.company_csv)
        marker_file = (
            company_path.parent / f".imported_all_{company_path.stem}.json"
        )
        return luigi.LocalTarget(str(marker_file))

    def run(self):
        """Save combined import results."""
        results = {
            "status": "completed",
            "imports": {
                "companies": str(self.input()[0].path),
                "leads": str(self.input()[1].path)
            }
        }

        # Read individual import results
        with open(self.input()[0].path, encoding="utf-8") as f:
            results["companies"] = json.load(f)
        with open(self.input()[1].path, encoding="utf-8") as f:
            results["leads"] = json.load(f)

        # Save combined results
        with open(self.output().path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)

        logger.info("Completed all HubSpot imports")
