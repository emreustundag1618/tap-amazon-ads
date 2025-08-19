# tap-amazon-ads

A Singer tap for Amazon Ads API, built with the Meltano Singer SDK.

[![Python version](https://img.shields.io/badge/python-^3.9-blue.svg)](https://python.org)
[![Singer SDK](https://img.shields.io/badge/singer--sdk-^0.47.4-green.svg)](https://sdk.meltano.com)

## Overview

`tap-amazon-ads` extracts data from the Amazon Advertising API for Sponsored Products campaigns. It provides access to campaigns, ad groups, keywords, product ads, targets, and negative keywords data.

### Key Features

- **OAuth2 Authentication** with Amazon Login with Amazon (LWA)
- **Efficient Parent-Child Streams** to avoid large data dumps
- **Campaign-filtered Keywords** for better performance  
- **Multiple Profile Support** for different marketplaces
- **Comprehensive Amazon Ads API Coverage**

### Supported Streams

| Stream | Description | Type | Parent Stream |
|--------|-------------|------|---------------|
| `campaigns` | Sponsored Products campaigns | Parent | - |
| `adgroups` | Ad groups within campaigns | Parent | - |
| `keywords` | Keywords with campaign filtering | Child | campaigns |
| `productads` | Product ads with ad group filtering | Child | adgroups |
| `targets` | Product targeting with campaign filtering | Child | campaigns |
| `negative_keywords` | Negative keywords with campaign filtering | Child | campaigns |

## Prerequisites

### Amazon Ads API Access

1. **Amazon Ads Account**: You need an active Amazon Advertising account
2. **API Access**: Register for Amazon Ads API access through the Amazon Advertising console
3. **Developer Application**: Create a developer application to get your credentials

### Required Credentials

You'll need to obtain these from Amazon Ads API:

- `client_id` - Your application's client ID (starts with `amzn1.application-oa2-client.`)
- `client_secret` - Your application's client secret (starts with `amzn1.oa2-cs.v1.`)
- `refresh_token` - Long-lived refresh token obtained during OAuth flow
- `profile_ids` - Array of profile IDs for the marketplaces you want to access

## Installation & Setup

### Option 1: Using with Meltano (Recommended)

1. **Add the tap to your Meltano project:**

```bash
# Add the custom tap to your meltano.yml
docker run -v ${pwd}:/projects -w /projects meltano/meltano add extractor tap-amazon-ads --custom
```

2. **Configure in `meltano.yml`:**

```yaml
extractors:
- name: tap-amazon-ads
  namespace: tap_amazon_ads
  executable: tap-amazon-ads
  pip_url: git+https://github.com/your-username/tap-amazon-ads.git
  capabilities:
  - about
  - catalog
  - discover
  - schema-flattening
  - state
  - stream-maps
  settings:
  - name: client_id
    label: Client ID
    kind: password
    description: Amazon Ads API Client ID
  - name: client_secret
    label: Client Secret  
    kind: password
    description: Amazon Ads API Client Secret
  - name: refresh_token
    label: Refresh Token
    kind: password
    description: Amazon Ads API Refresh Token
  - name: profile_ids
    label: Profile IDs
    kind: array
    description: List of Amazon Ads Profile IDs to extract data from
  - name: api_url
    label: API URL
    kind: string
    default: "https://advertising-api.amazon.com"
    description: Amazon Ads API base URL
  - name: auth_endpoint
    label: Auth Endpoint
    kind: string
    default: "https://api.amazon.com/auth/o2/token"
    description: Amazon OAuth2 token endpoint
  - name: start_date
    label: Start Date
    kind: date_iso8601
    description: Start date for data extraction
```

3. **Configure your credentials:**

```bash
# Set your Amazon Ads API credentials
docker run -v ${pwd}:/projects -w /projects meltano/meltano config tap-amazon-ads set client_id "your_client_id"
docker run -v ${pwd}:/projects -w /projects meltano/meltano config tap-amazon-ads set client_secret "your_client_secret"  
docker run -v ${pwd}:/projects -w /projects meltano/meltano config tap-amazon-ads set refresh_token "your_refresh_token"
docker run -v ${pwd}:/projects -w /projects meltano/meltano config tap-amazon-ads set profile_ids '["profile_id_1", "profile_id_2"]'
```

4. **Test the configuration:**

```bash
# Test connection and discover streams
docker run -v ${pwd}:/projects -w /projects meltano/meltano invoke tap-amazon-ads --discover

# Test extraction
docker run -v ${pwd}:/projects -w /projects meltano/meltano invoke tap-amazon-ads --config=config.json | head -20
```

### Option 2: Standalone Installation

1. **Clone and install:**

```bash
git clone https://github.com/your-username/tap-amazon-ads.git
cd tap-amazon-ads
pip install -e .
```

2. **Create configuration file:**

Copy `config.sample.json` to `config.json` and fill in your credentials:

```json
{
  "client_id": "amzn1.application-oa2-client.your_client_id",
  "client_secret": "amzn1.oa2-cs.v1.your_client_secret", 
  "refresh_token": "your_refresh_token",
  "profile_ids": ["profile_id_1", "profile_id_2"],
  "api_url": "https://advertising-api.amazon.com",
  "auth_endpoint": "https://api.amazon.com/auth/o2/token",
  "start_date": "2024-01-01T00:00:00Z"
}
```

3. **Run the tap:**

```bash
# Discover streams
tap-amazon-ads --config=config.json --discover

# Extract data  
tap-amazon-ads --config=config.json --catalog=catalog.json
```

## Configuration

### Required Settings

- **`client_id`** (string): Amazon Ads API Client ID
- **`client_secret`** (string): Amazon Ads API Client Secret  
- **`refresh_token`** (string): Amazon Ads API Refresh Token
- **`profile_ids`** (array): List of Amazon Ads Profile IDs

### Optional Settings

- **`api_url`** (string): Amazon Ads API base URL (default: `https://advertising-api.amazon.com`)
- **`auth_endpoint`** (string): OAuth2 token endpoint (default: `https://api.amazon.com/auth/o2/token`)
- **`start_date`** (string): Start date for data extraction (ISO 8601 format)
- **`permission_scope`** (string): OAuth permission scope (default: `advertising::campaign_management`)

## Usage Examples

### Basic Meltano Pipeline

```bash
# Select specific streams and fields
docker run -v ${pwd}:/projects -w /projects meltano/meltano select tap-amazon-ads campaigns.* keywords.keywordId keywords.keywordText keywords.bid

# Run full pipeline to load into your target
docker run -v ${pwd}:/projects -w /projects meltano/meltano run tap-amazon-ads target-postgres

# Run with specific date range
docker run -v ${pwd}:/projects -w /projects meltano/meltano run tap-amazon-ads target-postgres --start-date=2024-01-01
```

### Advanced Stream Selection

```bash
# Select campaigns and their related keywords only
docker run -v ${pwd}:/projects -w /projects meltano/meltano select tap-amazon-ads campaigns.* keywords.*

# Include product targeting data
docker run -v ${pwd}:/projects -w /projects meltano/meltano select tap-amazon-ads campaigns.* targets.* negative_keywords.*

# Full data extraction
docker run -v ${pwd}:/projects -w /projects meltano/meltano select tap-amazon-ads "*.*"
```

## Stream Relationships

The tap uses efficient parent-child stream relationships to minimize API calls:

```
campaigns (parent)
├── keywords (child) - filtered by campaign_id
├── targets (child) - filtered by campaign_id  
└── negative_keywords (child) - filtered by campaign_id

adgroups (parent)
└── productads (child) - filtered by adgroup_id
```

This architecture prevents fetching massive unfiltered datasets and improves performance significantly.

## Performance Considerations

- **Campaign Filtering**: Keywords, targets, and negative keywords are fetched per campaign to avoid large data dumps
- **Rate Limiting**: The tap respects Amazon Ads API rate limits with automatic retries
- **Pagination**: Large result sets are automatically paginated
- **Profile Iteration**: Data is fetched for each configured profile separately

## Troubleshooting

### Authentication Issues

**Error: 401 Unauthorized**
- Verify your `client_id`, `client_secret`, and `refresh_token` are correct
- Check that your refresh token hasn't expired
- Ensure your application has the correct permissions

**Error: 403 Forbidden**  
- Verify your profile IDs are correct and accessible
- Check that your application has access to the specified profiles
- Ensure you have the right permissions for the requested data

### Data Issues

**Error: No data returned**
- Check your `start_date` configuration
- Verify that campaigns exist in the specified date range
- Ensure campaigns are in ENABLED or PAUSED state (ARCHIVED campaigns may not return data)

**Error: Large keyword datasets**
- The tap uses campaign filtering to avoid this issue
- If you still experience performance issues, consider limiting your profile IDs

### API Limits

- Amazon Ads API has rate limits per profile
- The tap includes automatic retry logic with exponential backoff
- For high-volume extractions, consider running during off-peak hours

## Development

### Setting up development environment

1. **Clone and setup:**

```bash
git clone https://github.com/your-username/tap-amazon-ads.git
cd tap-amazon-ads
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e .[dev]
```

2. **Run tests:**

```bash
pytest tests/
```

3. **Code formatting:**

```bash
pre-commit install
pre-commit run --all-files
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Run the test suite
5. Submit a pull request

## Related Projects

- **Singer SDK**: https://sdk.meltano.com
- **Meltano**: https://meltano.com
- **Amazon Ads API Documentation**: https://advertising.amazon.com/API/docs

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

## Support

- **Issues**: Please open an issue on GitHub for bug reports or feature requests
- **Documentation**: Check the Amazon Ads API documentation for API-specific questions
- **Community**: Join the Meltano Slack community for general questions

---

Built with ❤️ using the [Meltano Singer SDK](https://sdk.meltano.com)
