"""
LinkedIn API client using Voyager (internal) and Sales Navigator APIs.
Requires li_at cookie for authentication.
"""

import httpx
import re
import urllib.parse
import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)

VOYAGER_BASE = "https://www.linkedin.com/voyager/api"
SALES_NAV_BASE = "https://www.linkedin.com/sales-api"

COMMON_HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "accept": "application/vnd.linkedin.normalized+json+2.1",
    "accept-language": "en-US,en;q=0.9",
    "x-li-lang": "en_US",
    "x-restli-protocol-version": "2.0.0",
}

BROWSER_HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9,nl;q=0.8",
}


def _build_cookies(li_at: str, li_a: Optional[str] = None) -> dict:
    cookies = {"li_at": li_at, "JSESSIONID": '"ajax:0"'}
    if li_a:
        cookies["li_a"] = li_a
    return cookies


def _build_headers(csrf_token: str = "ajax:0") -> dict:
    return {**COMMON_HEADERS, "csrf-token": csrf_token}


def _clean_domain(website: str) -> str:
    """Extract clean domain from a URL."""
    domain = website.strip().lower()
    for prefix in ["https://", "http://", "www."]:
        if domain.startswith(prefix):
            domain = domain[len(prefix):]
    domain = domain.split("/")[0].split("?")[0].split("#")[0]
    return domain


def _domain_to_brand(domain: str) -> str:
    """Extract a human-readable brand name from a domain.
    e.g. 'sanitairdesigncenter.nl' -> 'sanitair design center'
         'rjs-badkamers.nl' -> 'rjs badkamers'
         'vdbergbadkamers.nl' -> 'vdberg badkamers'
    """
    name = domain.split(".")[0]  # strip TLD
    # Split on hyphens
    name = name.replace("-", " ")
    # Insert spaces before camelCase transitions and before common Dutch/English suffixes
    name = re.sub(r'([a-z])([A-Z])', r'\1 \2', name)
    return name.strip()


async def _scrape_website_for_linkedin(client: httpx.AsyncClient, website: str) -> Optional[str]:
    """Scrape the target website's HTML to find a LinkedIn company link."""
    url = website.strip()
    if not url.startswith("http"):
        url = "https://" + url
    # Strip path params to get base URL too
    from urllib.parse import urlparse
    parsed = urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    urls_to_try = [url]
    if url != base_url and url != base_url + "/":
        urls_to_try.append(base_url)

    for try_url in urls_to_try:
        try:
            resp = await client.get(try_url, headers=BROWSER_HEADERS, timeout=10, follow_redirects=True)
            if resp.status_code != 200:
                continue
            html = resp.text
            # Look for linkedin.com/company/ links
            matches = re.findall(
                r'https?://(?:www\.)?linkedin\.com/company/([a-zA-Z0-9_-]+)',
                html
            )
            if matches:
                slug = matches[0].rstrip("/")
                return slug
        except Exception as e:
            logger.debug(f"Scrape failed for {try_url}: {e}")
            continue
    return None


async def _resolve_slug_to_company(
    client: httpx.AsyncClient, slug: str, cookies: dict, headers: dict
) -> Optional[dict]:
    """Given a LinkedIn company slug, resolve it to a company dict with URN."""
    # First try the public page to get the URN
    url = f"https://www.linkedin.com/company/{urllib.parse.quote(slug, safe='-_.~')}/"
    try:
        resp = await client.get(url, headers=BROWSER_HEADERS, timeout=12, follow_redirects=True)
        if resp.status_code == 200:
            html = resp.text
            for pattern in [
                r'"objectUrn":"(urn:li:organization:\d+)"',
                r'"objectUrn":"(urn:li:company:\d+)"',
                r'urn:li:fs_normalized_company:(\d+)',
                r'"companyId":(\d+)',
                r'"organizationId":(\d+)',
            ]:
                m = re.search(pattern, html)
                if m:
                    val = m.group(1)
                    urn = val if val.startswith("urn:") else f"urn:li:company:{val}"
                    # Try to extract name
                    name_match = re.search(r'<title>([^<|]+)', html)
                    name = name_match.group(1).strip().split(" | ")[0] if name_match else slug
                    return {
                        "companyName": name,
                        "companyUrn": urn,
                        "companyUrl": f"https://www.linkedin.com/company/{slug}/",
                    }
    except Exception as e:
        logger.debug(f"Public page scrape failed for {slug}: {e}")

    # Fallback: use Voyager to look up the slug directly
    try:
        voyager_url = f"{VOYAGER_BASE}/organization/companies?decorationId=com.linkedin.voyager.deco.organization.web.WebFullCompanyMain-12&q=universalName&universalName={slug}"
        resp = await client.get(voyager_url, cookies=cookies, headers=headers, timeout=12)
        if resp.status_code == 200:
            data = resp.json()
            elements = data.get("elements", [])
            if elements:
                elem = elements[0]
                return {
                    "companyName": elem.get("name", slug),
                    "companyUrn": elem.get("entityUrn", ""),
                    "companyUrl": f"https://www.linkedin.com/company/{slug}/",
                }
    except Exception as e:
        logger.debug(f"Voyager slug lookup failed for {slug}: {e}")

    # Still return something usable with the slug
    return {
        "companyName": slug.replace("-", " ").title(),
        "companyUrn": "",
        "companyUrl": f"https://www.linkedin.com/company/{slug}/",
    }


async def search_company_by_website(
    client: httpx.AsyncClient, website: str, li_at: str, li_a: Optional[str] = None
) -> Optional[dict]:
    """
    Search for a LinkedIn company page using the website domain.
    Returns dict with companyName, companyUrn, companyUrl, or None.

    Strategy order:
    1. Scrape the website HTML for a linkedin.com/company/ link (most reliable)
    2. Voyager company search with the domain
    3. Voyager company search with extracted brand name
    4. Voyager typeahead with brand name
    """
    domain = _clean_domain(website)
    brand = _domain_to_brand(domain)
    cookies = _build_cookies(li_at, li_a)
    headers = _build_headers()

    # Strategy 1: Scrape website for LinkedIn company link
    try:
        slug = await _scrape_website_for_linkedin(client, website)
        if slug:
            logger.info(f"Found LinkedIn slug from website scrape: {slug}")
            result = await _resolve_slug_to_company(client, slug, cookies, headers)
            if result:
                return result
    except Exception as e:
        logger.warning(f"Website scrape failed for {website}: {e}")

    # Strategy 2: Voyager search with full domain
    try:
        result = await _voyager_company_search(client, domain, cookies, headers)
        if result:
            return result
    except Exception as e:
        logger.warning(f"Voyager company search failed for {domain}: {e}")

    # Strategy 3: Voyager search with brand name (if different from domain)
    if brand != domain.split(".")[0]:
        try:
            result = await _voyager_company_search(client, brand, cookies, headers)
            if result:
                return result
        except Exception as e:
            logger.warning(f"Voyager brand search failed for {brand}: {e}")

    # Strategy 4: Voyager typeahead with brand name
    try:
        result = await _voyager_typeahead_search(client, brand, cookies, headers)
        if result:
            return result
    except Exception as e:
        logger.warning(f"Voyager typeahead failed for {brand}: {e}")

    # Strategy 5: Voyager typeahead with just the first word of brand
    first_word = brand.split()[0] if brand else ""
    if first_word and len(first_word) > 3 and first_word != brand:
        try:
            result = await _voyager_typeahead_search(client, first_word, cookies, headers)
            if result:
                return result
        except Exception as e:
            logger.warning(f"Voyager typeahead (first word) failed for {first_word}: {e}")

    return None


async def _voyager_company_search(
    client: httpx.AsyncClient, domain: str, cookies: dict, headers: dict
) -> Optional[dict]:
    """Use Voyager search API to find company by domain keyword."""
    params = {
        "decorationId": "com.linkedin.voyager.deco.jserp.WebSearchClusterCollection-14",
        "origin": "SWITCH_SEARCH_VERTICAL",
        "q": "all",
        "keywords": domain,
        "queryParameters.resultType": "List(COMPANIES)",
        "start": "0",
        "count": "5",
    }

    url = f"{VOYAGER_BASE}/search/dash/clusters"
    resp = await client.get(url, params=params, cookies=cookies, headers=headers)

    if resp.status_code != 200:
        logger.warning(f"Company search returned {resp.status_code}")
        return None

    data = resp.json()
    included = data.get("included", [])

    # Look through included entities for company results
    for item in included:
        entity_urn = item.get("entityUrn", "")
        if "fsd_company" in entity_urn or "company" in entity_urn.lower():
            company_name = item.get("title", {}).get("text", "") if isinstance(item.get("title"), dict) else item.get("title", "")
            navigation_url = item.get("navigationUrl", "")

            # Extract company URN
            urn = None
            if "fsd_company:" in entity_urn:
                urn = entity_urn
            elif "company:" in entity_urn:
                urn = entity_urn

            if urn and company_name:
                return {
                    "companyName": company_name,
                    "companyUrn": urn,
                    "companyUrl": navigation_url or f"https://www.linkedin.com/company/{urn.split(':')[-1]}/",
                }

        # Also check for mini company objects
        if item.get("$type", "") == "com.linkedin.voyager.entities.shared.MiniCompany" or \
           item.get("$type", "") == "com.linkedin.voyager.organization.Company":
            universal_name = item.get("universalName", "")
            name = item.get("name", "")
            urn = item.get("entityUrn", item.get("objectUrn", ""))
            if name:
                return {
                    "companyName": name,
                    "companyUrn": urn,
                    "companyUrl": f"https://www.linkedin.com/company/{universal_name or urn.split(':')[-1]}/",
                }

    # Fallback: look for any entity with matching website in included
    for item in included:
        websites = item.get("companyPageUrl", "") or item.get("websiteUrl", "")
        if domain in str(websites).lower():
            name = item.get("name", item.get("title", ""))
            if isinstance(name, dict):
                name = name.get("text", "")
            urn = item.get("entityUrn", item.get("objectUrn", ""))
            universal_name = item.get("universalName", "")
            if name:
                return {
                    "companyName": name,
                    "companyUrn": urn,
                    "companyUrl": f"https://www.linkedin.com/company/{universal_name or urn.split(':')[-1]}/",
                }

    return None


async def _voyager_typeahead_search(
    client: httpx.AsyncClient, domain: str, cookies: dict, headers: dict
) -> Optional[dict]:
    """Use Voyager typeahead to find company."""
    params = {
        "decorationId": "com.linkedin.voyager.deco.jserp.WebSearchClusterCollection-14",
        "origin": "SWITCH_SEARCH_VERTICAL",
        "q": "all",
        "keywords": domain.split(".")[0],  # Use just the brand name part
        "queryParameters.resultType": "List(COMPANIES)",
        "start": "0",
        "count": "5",
    }

    url = f"{VOYAGER_BASE}/search/dash/clusters"
    resp = await client.get(url, params=params, cookies=cookies, headers=headers)

    if resp.status_code != 200:
        return None

    data = resp.json()
    included = data.get("included", [])

    for item in included:
        # Check for company mini profiles
        item_type = item.get("$type", "")
        if "MiniCompany" in item_type or "Company" in item_type:
            name = item.get("name", "")
            urn = item.get("entityUrn", item.get("objectUrn", ""))
            universal_name = item.get("universalName", "")
            if name:
                return {
                    "companyName": name,
                    "companyUrn": urn,
                    "companyUrl": f"https://www.linkedin.com/company/{universal_name or urn.split(':')[-1]}/",
                }

        # Check search result entities
        entity_urn = item.get("entityUrn", "")
        if "fsd_company" in entity_urn:
            title = item.get("title", {})
            company_name = title.get("text", "") if isinstance(title, dict) else str(title)
            if company_name:
                return {
                    "companyName": company_name,
                    "companyUrn": entity_urn,
                    "companyUrl": item.get("navigationUrl", f"https://www.linkedin.com/company/{entity_urn.split(':')[-1]}/"),
                }

    return None


def _extract_company_id(urn: str) -> str:
    """Extract numeric company ID from a URN string."""
    # Handle formats like: urn:li:fsd_company:12345, urn:li:company:12345, fsd_company:12345
    parts = urn.split(":")
    for i, part in enumerate(parts):
        if part in ("fsd_company", "company", "fs_salesCompany") and i + 1 < len(parts):
            return parts[i + 1]
    # Fallback: return last numeric segment
    for part in reversed(parts):
        if part.isdigit():
            return part
    return urn


async def search_leads_sales_nav(
    client: httpx.AsyncClient,
    company_urn: str,
    li_at: str,
    li_a: Optional[str] = None,
    seniority_levels: Optional[list[str]] = None,
    title_keywords: Optional[str] = None,
    function_ids: Optional[list[str]] = None,
    count: int = 25,
) -> list[dict]:
    """
    Search Sales Navigator for leads at a specific company.
    Returns list of lead dicts.
    """
    company_id = _extract_company_id(company_urn)
    cookies = _build_cookies(li_at, li_a)
    headers = {**_build_headers(), "accept": "application/json"}

    # Try Sales Nav search first, fall back to Voyager people search
    leads = await _sales_nav_lead_search(
        client, company_id, cookies, headers,
        seniority_levels=seniority_levels,
        title_keywords=title_keywords,
        function_ids=function_ids,
        count=count,
    )

    if not leads:
        leads = await _voyager_people_search(
            client, company_id, cookies, headers,
            title_keywords=title_keywords,
            count=count,
        )

    return leads


async def _sales_nav_lead_search(
    client: httpx.AsyncClient,
    company_id: str,
    cookies: dict,
    headers: dict,
    seniority_levels: Optional[list[str]] = None,
    title_keywords: Optional[str] = None,
    function_ids: Optional[list[str]] = None,
    count: int = 25,
) -> list[dict]:
    """Search for leads using Sales Navigator API."""

    # Build the Sales Nav search query
    query_parts = []

    # Company filter
    query_parts.append(f"(type:COMPANY_ID,values:List({company_id}))")

    # Seniority filter
    if seniority_levels:
        seniority_str = ",".join(seniority_levels)
        query_parts.append(f"(type:SENIORITY_LEVEL,values:List({seniority_str}))")

    # Function filter
    if function_ids:
        func_str = ",".join(function_ids)
        query_parts.append(f"(type:FUNCTION,values:List({func_str}))")

    filters = "List(" + ",".join(query_parts) + ")"

    params = {
        "q": "peopleSearchQuery",
        "query": f"(filters:{filters})",
        "start": "0",
        "count": str(count),
        "decorationId": "com.linkedin.sales.deco.desktop.searchv2.LeadSearchResult-14",
    }

    if title_keywords:
        params["query"] = f"(keywords:{urllib.parse.quote(title_keywords)},filters:{filters})"

    url = f"{SALES_NAV_BASE}/salesApiLeadSearch"
    resp = await client.get(url, params=params, cookies=cookies, headers=headers)

    if resp.status_code != 200:
        logger.warning(f"Sales Nav search returned {resp.status_code}")
        # Try alternative Sales Nav endpoint
        return await _sales_nav_search_v2(
            client, company_id, cookies, headers,
            seniority_levels, title_keywords, function_ids, count
        )

    data = resp.json()
    leads = []

    elements = data.get("elements", [])
    for elem in elements:
        lead = _parse_sales_nav_lead(elem)
        if lead:
            leads.append(lead)

    return leads


async def _sales_nav_search_v2(
    client: httpx.AsyncClient,
    company_id: str,
    cookies: dict,
    headers: dict,
    seniority_levels: Optional[list[str]] = None,
    title_keywords: Optional[str] = None,
    function_ids: Optional[list[str]] = None,
    count: int = 25,
) -> list[dict]:
    """Alternative Sales Nav endpoint."""

    # Build pivot-based query for newer Sales Nav API
    query_filters = [
        {"type": "COMPANY_ID", "values": [{"id": company_id}]}
    ]

    if seniority_levels:
        query_filters.append({
            "type": "SENIORITY_LEVEL",
            "values": [{"id": s} for s in seniority_levels]
        })

    if function_ids:
        query_filters.append({
            "type": "FUNCTION",
            "values": [{"id": f} for f in function_ids]
        })

    search_params = {
        "filters": "List(" + ",".join(
            f"(type:{f['type']},values:List({','.join(v['id'] for v in f['values'])}))"
            for f in query_filters
        ) + ")",
        "start": 0,
        "count": count,
        "q": "peopleSearchQuery",
    }

    if title_keywords:
        search_params["keywords"] = title_keywords

    url = f"{SALES_NAV_BASE}/salesApiPeopleSearch"
    resp = await client.get(url, params=search_params, cookies=cookies, headers=headers)

    if resp.status_code != 200:
        logger.warning(f"Sales Nav v2 search returned {resp.status_code}")
        return []

    data = resp.json()
    leads = []

    for elem in data.get("elements", []):
        lead = _parse_sales_nav_lead(elem)
        if lead:
            leads.append(lead)

    # Also check included for nested entities
    for item in data.get("included", []):
        if "fsd_profile" in item.get("entityUrn", ""):
            lead = _parse_included_profile(item)
            if lead and lead not in leads:
                leads.append(lead)

    return leads


def _parse_sales_nav_lead(elem: dict) -> Optional[dict]:
    """Parse a Sales Nav lead search result element."""
    current_positions = elem.get("currentPositions", [])
    title = ""
    if current_positions:
        title = current_positions[0].get("title", "")

    first_name = elem.get("firstName", "")
    last_name = elem.get("lastName", "")
    full_name = f"{first_name} {last_name}".strip()

    if not full_name:
        full_name = elem.get("fullName", "")

    if not full_name:
        return None

    # Get profile URL
    profile_id = elem.get("publicIdentifier", "") or elem.get("profileId", "")
    entity_urn = elem.get("entityUrn", "")

    profile_url = ""
    if profile_id:
        profile_url = f"https://www.linkedin.com/in/{profile_id}/"
    elif entity_urn:
        # Extract from URN
        parts = entity_urn.split(":")
        if parts:
            profile_url = f"https://www.linkedin.com/in/{parts[-1]}/"

    return {
        "name": full_name,
        "designation": title or elem.get("title", ""),
        "profileUrl": profile_url,
    }


def _parse_included_profile(item: dict) -> Optional[dict]:
    """Parse a profile from the included array."""
    first = item.get("firstName", "")
    last = item.get("lastName", "")
    name = f"{first} {last}".strip() or item.get("fullName", "")

    if not name:
        return None

    title = item.get("title", "") or item.get("headline", "")
    public_id = item.get("publicIdentifier", "")
    profile_url = f"https://www.linkedin.com/in/{public_id}/" if public_id else ""

    return {
        "name": name,
        "designation": title,
        "profileUrl": profile_url,
    }


async def _voyager_people_search(
    client: httpx.AsyncClient,
    company_id: str,
    cookies: dict,
    headers: dict,
    title_keywords: Optional[str] = None,
    count: int = 25,
) -> list[dict]:
    """Fallback: search for people at a company using Voyager API."""

    keywords = title_keywords or ""

    params = {
        "decorationId": "com.linkedin.voyager.deco.jserp.WebSearchClusterCollection-14",
        "origin": "FACETED_SEARCH",
        "q": "all",
        "keywords": keywords,
        "queryParameters.currentCompany": f"List({company_id})",
        "queryParameters.resultType": "List(PEOPLE)",
        "start": "0",
        "count": str(count),
    }

    url = f"{VOYAGER_BASE}/search/dash/clusters"
    resp = await client.get(url, params=params, cookies=cookies, headers=headers)

    if resp.status_code != 200:
        logger.warning(f"Voyager people search returned {resp.status_code}")
        return []

    data = resp.json()
    included = data.get("included", [])
    leads = []

    for item in included:
        item_type = item.get("$type", "")
        entity_urn = item.get("entityUrn", "")

        # Look for profile entities
        if "MiniProfile" in item_type or "fsd_profile" in entity_urn:
            first = item.get("firstName", "")
            last = item.get("lastName", "")
            name = f"{first} {last}".strip()
            occupation = item.get("occupation", "")
            public_id = item.get("publicIdentifier", "")
            profile_url = f"https://www.linkedin.com/in/{public_id}/" if public_id else ""

            if name:
                leads.append({
                    "name": name,
                    "designation": occupation,
                    "profileUrl": profile_url,
                })

        # Also check search result entities with title/subtitle
        if "fsd_profile" in entity_urn or "fsd_entityResultViewModel" in entity_urn:
            title_obj = item.get("title", {})
            name = title_obj.get("text", "") if isinstance(title_obj, dict) else str(title_obj or "")
            subtitle = item.get("primarySubtitle", {})
            designation = subtitle.get("text", "") if isinstance(subtitle, dict) else str(subtitle or "")
            nav_url = item.get("navigationUrl", "")

            if name and name not in [l["name"] for l in leads]:
                profile_url = ""
                if nav_url and "linkedin.com/in/" in nav_url:
                    profile_url = nav_url.split("?")[0]

                leads.append({
                    "name": name,
                    "designation": designation,
                    "profileUrl": profile_url,
                })

    return leads[:count]
