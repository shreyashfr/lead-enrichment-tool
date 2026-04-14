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


# Seniority name -> Sales Nav numeric ID mapping
SENIORITY_MAP = {
    "CXO": "7",
    "VP": "8",
    "DIRECTOR": "9",
    "MANAGER": "10",
    "SENIOR": "4",
    "ENTRY": "3",
}


def _normalize_urn(urn: str) -> str:
    """Normalize company URN to urn:li:organization:ID format."""
    if not urn:
        return ""
    # Extract the numeric ID from any URN format
    parts = urn.split(":")
    for i, part in enumerate(parts):
        if part in ("fsd_company", "company", "organization", "fs_salesCompany",
                     "fs_normalized_company") and i + 1 < len(parts):
            num_id = parts[i + 1]
            return f"urn:li:organization:{num_id}"
    # Fallback: extract last numeric segment
    for part in reversed(parts):
        if part.isdigit():
            return f"urn:li:organization:{part}"
    return urn


async def search_leads_sales_nav(
    client: httpx.AsyncClient,
    company_urn: str,
    company_name: str,
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
    cookies = _build_cookies(li_at, li_a)
    headers = {
        **_build_headers(),
        "accept": "application/vnd.linkedin.normalized+json+2.1",
        "referer": "https://www.linkedin.com/sales/search/people",
    }

    normalized_urn = _normalize_urn(company_urn)

    # Convert seniority names to numeric IDs
    seniority_ids = None
    if seniority_levels:
        seniority_ids = [SENIORITY_MAP.get(s.upper(), s) for s in seniority_levels]

    # Try Sales Nav search first
    if normalized_urn:
        leads = await _sales_nav_lead_search(
            client, normalized_urn, company_name, cookies, headers,
            seniority_ids=seniority_ids,
            title_keywords=title_keywords,
            function_ids=function_ids,
            count=count,
        )
        if leads:
            return leads

        # Retry without seniority filter if no results
        if seniority_ids:
            logger.info(f"No leads with seniority filter, retrying without for {company_name}")
            leads = await _sales_nav_lead_search(
                client, normalized_urn, company_name, cookies, headers,
                seniority_ids=None,
                title_keywords=title_keywords,
                function_ids=function_ids,
                count=count,
            )
            if leads:
                return leads

    # Fallback to Voyager people search
    company_id = normalized_urn.split(":")[-1] if normalized_urn else ""
    if company_id:
        leads = await _voyager_people_search(
            client, company_id, cookies, headers,
            title_keywords=title_keywords,
            count=count,
        )
        if leads:
            return leads

    return []


async def _sales_nav_lead_search(
    client: httpx.AsyncClient,
    company_urn: str,
    company_name: str,
    cookies: dict,
    headers: dict,
    seniority_ids: Optional[list[str]] = None,
    title_keywords: Optional[str] = None,
    function_ids: Optional[list[str]] = None,
    count: int = 25,
) -> list[dict]:
    """Search for leads using Sales Navigator API matching the working bot's format."""

    # Encode URN: urn:li:organization:123 -> urn%3Ali%3Aorganization%3A123
    encoded_urn = company_urn.replace(":", "%3A")

    # Build filters in exact format the working bot uses
    filter_parts = []

    # Company filter: (type:CURRENT_COMPANY,values:List((id:urn%3Ali%3Aorganization%3A123,text:CompanyName,selectionType:INCLUDED)))
    safe_name = company_name.replace(",", " ").replace("(", "").replace(")", "")
    filter_parts.append(
        f"(type:CURRENT_COMPANY,values:List((id:{encoded_urn},text:{safe_name},selectionType:INCLUDED)))"
    )

    # Seniority filter
    if seniority_ids:
        seniority_values = ",".join(
            f"(id:{sid},text:{sid},selectionType:INCLUDED)" for sid in seniority_ids
        )
        filter_parts.append(f"(type:SENIORITY_LEVEL,values:List({seniority_values}))")

    # Function filter
    if function_ids:
        func_values = ",".join(
            f"(id:{fid},text:{fid},selectionType:INCLUDED)" for fid in function_ids
        )
        filter_parts.append(f"(type:FUNCTION,values:List({func_values}))")

    filters_str = "List(" + ",".join(filter_parts) + ")"

    # Build query
    if title_keywords:
        query_str = f"(keywords:{urllib.parse.quote(title_keywords)},filters:{filters_str})"
    else:
        query_str = f"(filters:{filters_str})"

    params = {
        "q": "searchQuery",
        "query": query_str,
        "start": "0",
        "count": str(count),
        "decorationId": "com.linkedin.sales.deco.desktop.searchv2.LeadSearchResult-14",
    }

    url = f"{SALES_NAV_BASE}/salesApiLeadSearch"
    logger.info(f"Sales Nav search: {company_name} (URN: {company_urn})")

    resp = await client.get(url, params=params, cookies=cookies, headers=headers)

    if resp.status_code == 429:
        logger.warning("Sales Nav rate limited (429)")
        await asyncio.sleep(30)
        resp = await client.get(url, params=params, cookies=cookies, headers=headers)

    if resp.status_code != 200:
        logger.warning(f"Sales Nav search returned {resp.status_code} for {company_name}")
        return []

    data = resp.json()
    return _parse_sales_nav_response(data)


def _parse_sales_nav_response(data: dict) -> list[dict]:
    """Parse Sales Nav response - extract leads from included array."""
    leads = []
    seen_names = set()

    included = data.get("included", [])

    # Build a map of URN -> profile data from included
    for item in included:
        item_type = item.get("$type", "")
        entity_urn = item.get("entityUrn", "")

        # Match DecoratedPeopleSearchHit or similar profile objects
        is_search_hit = "SearchHit" in item_type or "LeadSearchResult" in item_type
        is_profile = "fsd_profile" in entity_urn or "MiniProfile" in item_type

        if not (is_search_hit or is_profile):
            continue

        # Extract name
        full_name = item.get("fullName", "")
        if not full_name:
            first = item.get("firstName", "")
            last = item.get("lastName", "")
            full_name = f"{first} {last}".strip()

        if not full_name or full_name in seen_names:
            continue

        # Extract title from currentPositions or headline
        title = ""
        current_positions = item.get("currentPositions", [])
        if current_positions:
            title = current_positions[0].get("title", "")
        if not title:
            title = item.get("title", "") or item.get("headline", "") or item.get("summary", "")

        # Extract profile URL - try multiple fields
        profile_url = item.get("publicProfileUrl", "")
        if not profile_url:
            vanity = item.get("vanityName", "") or item.get("publicIdentifier", "")
            if vanity:
                profile_url = f"https://www.linkedin.com/in/{vanity}/"

        if full_name:
            seen_names.add(full_name)
            leads.append({
                "name": full_name,
                "designation": title,
                "profileUrl": profile_url,
            })

    # Also try top-level elements if included was empty
    if not leads:
        for elem in data.get("elements", []):
            lead = _parse_element_lead(elem)
            if lead and lead["name"] not in seen_names:
                seen_names.add(lead["name"])
                leads.append(lead)

    return leads


def _parse_element_lead(elem: dict) -> Optional[dict]:
    """Parse a lead from a top-level element."""
    full_name = elem.get("fullName", "")
    if not full_name:
        first = elem.get("firstName", "")
        last = elem.get("lastName", "")
        full_name = f"{first} {last}".strip()

    if not full_name:
        return None

    title = ""
    current_positions = elem.get("currentPositions", [])
    if current_positions:
        title = current_positions[0].get("title", "")
    if not title:
        title = elem.get("title", "") or elem.get("headline", "")

    profile_url = elem.get("publicProfileUrl", "")
    if not profile_url:
        pid = elem.get("publicIdentifier", "") or elem.get("vanityName", "")
        if pid:
            profile_url = f"https://www.linkedin.com/in/{pid}/"

    return {
        "name": full_name,
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
    seen_names = set()

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

            if name and name not in seen_names:
                seen_names.add(name)
                leads.append({
                    "name": name,
                    "designation": occupation,
                    "profileUrl": profile_url,
                })

        # Also check search result entities with title/subtitle
        if "fsd_entityResultViewModel" in entity_urn:
            title_obj = item.get("title", {})
            name = title_obj.get("text", "") if isinstance(title_obj, dict) else str(title_obj or "")
            subtitle = item.get("primarySubtitle", {})
            designation = subtitle.get("text", "") if isinstance(subtitle, dict) else str(subtitle or "")
            nav_url = item.get("navigationUrl", "")

            if name and name not in seen_names:
                seen_names.add(name)
                profile_url = ""
                if nav_url and "linkedin.com/in/" in nav_url:
                    profile_url = nav_url.split("?")[0]

                leads.append({
                    "name": name,
                    "designation": designation,
                    "profileUrl": profile_url,
                })

    return leads[:count]
