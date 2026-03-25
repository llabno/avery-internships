"""
Weekly data update script — runs the LinkedIn scraper then merges results
into the main Chicago Audio Internships JSON/CSV files.
Designed to run in GitHub Actions CI.
"""

import csv
import json
import time
import re
from datetime import datetime
from urllib.parse import quote_plus
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# ── Config ──────────────────────────────────────────────────────────

SEARCH_QUERIES = [
    "audio engineer intern",
    "recording studio intern",
    "sound production intern",
    "audio intern",
    "music production intern",
    "studio intern",
    "sound engineer intern",
    "audio production intern",
    "music intern",
    "podcast production intern",
    "broadcast engineer intern",
    "media production intern",
    "post production intern",
    "live sound intern",
    "audio visual intern",
]

LOCATION = "Chicago, Illinois, United States"
BASE_URL = "https://www.linkedin.com/jobs/search"

# ── Scraper ─────────────────────────────────────────────────────────

def build_search_url(keywords, location, start=0):
    params = (
        f"keywords={quote_plus(keywords)}"
        f"&location={quote_plus(location)}"
        f"&f_E=1&position=1&pageNum=0&start={start}"
    )
    return f"{BASE_URL}?{params}"


def scroll_to_load_all(page):
    previous_count = 0
    stale_rounds = 0
    while stale_rounds < 3:
        try:
            see_more = page.locator("button.infinite-scroller__show-more-button")
            if see_more.is_visible(timeout=2000):
                see_more.click()
                page.wait_for_timeout(1500)
        except Exception:
            pass
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)
        current_count = page.locator("ul.jobs-search__results-list > li").count()
        if current_count == previous_count:
            stale_rounds += 1
        else:
            stale_rounds = 0
            previous_count = current_count
    print(f"    Loaded {previous_count} cards", flush=True)


def safe_text(locator):
    try:
        if locator.count():
            return locator.first.inner_text(timeout=3000).strip()
    except Exception:
        pass
    return ""


def safe_attr(locator, attr):
    try:
        if locator.count():
            return (locator.first.get_attribute(attr) or "").strip()
    except Exception:
        pass
    return ""


def extract_card_basics(card):
    title = safe_text(card.locator("h3.base-search-card__title"))
    company = safe_text(card.locator("h4.base-search-card__subtitle a, h4.base-search-card__subtitle"))
    location = safe_text(card.locator("span.job-search-card__location"))
    date_el = card.locator("time")
    posting_date = safe_attr(date_el, "datetime") or safe_text(date_el)
    posting_date_relative = safe_text(date_el)
    link_el = card.locator("a.base-card__full-link, a.base-search-card__full-link")
    raw_url = safe_attr(link_el, "href")
    job_url = raw_url.split("?")[0] if raw_url else ""
    job_id = ""
    match = re.search(r"-(\d+)$", job_url)
    if match:
        job_id = match.group(1)
    logo_el = card.locator("img.artdeco-entity-image, img[data-delayed-url]")
    company_logo = safe_attr(logo_el, "data-delayed-url") or safe_attr(logo_el, "src")
    company_link = safe_attr(card.locator("h4.base-search-card__subtitle a"), "href")
    if company_link:
        company_link = company_link.split("?")[0]
    benefits = safe_text(card.locator("span.result-benefits__text"))
    is_promoted = bool(card.locator("span.job-search-card__promoted-badge, span.result-benefits__promoted").count())
    return {
        "job_id": job_id, "title": title, "company": company,
        "company_url": company_link, "company_logo": company_logo,
        "location": location, "posting_date": posting_date,
        "posting_date_relative": posting_date_relative, "job_url": job_url,
        "is_promoted": is_promoted, "benefits_tag": benefits,
    }


def scrape_job_detail(page, job_url):
    detail = {
        "description_html": "", "description_text": "", "seniority_level": "",
        "employment_type": "", "job_function": "", "industries": "",
        "applicant_count": "", "salary": "", "apply_url": "",
    }
    if not job_url:
        return detail
    try:
        page.goto(job_url, wait_until="domcontentloaded", timeout=15000)
        page.wait_for_timeout(1500)
        try:
            show_more = page.locator("button.show-more-less-html__button--more")
            if show_more.is_visible(timeout=2000):
                show_more.click()
                page.wait_for_timeout(400)
        except Exception:
            pass
        desc_el = page.locator("div.show-more-less-html__markup, div.description__text")
        try:
            if desc_el.count():
                detail["description_html"] = desc_el.first.evaluate("el => el.innerHTML") or ""
        except Exception:
            pass
        detail["description_text"] = safe_text(desc_el)
        criteria_items = page.locator("ul.description__job-criteria-list > li")
        for idx in range(criteria_items.count()):
            item = criteria_items.nth(idx)
            header = safe_text(item.locator("h3")).lower()
            value = safe_text(item.locator("span.description__job-criteria-text"))
            if "seniority" in header:
                detail["seniority_level"] = value
            elif "employment" in header or "type" in header:
                detail["employment_type"] = value
            elif "function" in header:
                detail["job_function"] = value
            elif "industr" in header:
                detail["industries"] = value
        detail["applicant_count"] = safe_text(
            page.locator("span.num-applicants__caption, figcaption.num-applicants__caption"))
        detail["salary"] = safe_text(
            page.locator("div.salary, div.compensation__salary, span.compensation__salary"))
        apply_el = page.locator("a.apply-button, a[data-tracking-control-name*='apply']")
        detail["apply_url"] = safe_attr(apply_el, "href")
    except Exception as exc:
        print(f"      Detail error: {exc}", flush=True)
    return detail


def deduplicate(jobs):
    seen = set()
    unique = []
    for job in jobs:
        key = job["job_url"] or f"{job['title']}|{job['company']}"
        if key not in seen:
            seen.add(key)
            unique.append(job)
    return unique


def run_scraper():
    """Scrape LinkedIn and return list of job dicts."""
    all_cards = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()
        page.set_default_timeout(15000)

        # Phase 1: collect cards
        for query in SEARCH_QUERIES:
            print(f"\n--- Searching: \"{query}\"", flush=True)
            url = build_search_url(query, LOCATION)
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            try:
                page.wait_for_selector("ul.jobs-search__results-list > li", timeout=15000)
            except PlaywrightTimeout:
                print("    No results — skipping.", flush=True)
                continue
            scroll_to_load_all(page)
            cards = page.locator("ul.jobs-search__results-list > li")
            count = cards.count()
            for i in range(count):
                try:
                    basics = extract_card_basics(cards.nth(i))
                    basics["search_query"] = query
                    all_cards.append(basics)
                except Exception as exc:
                    print(f"    Card {i} error: {exc}", flush=True)
            time.sleep(2)

        all_cards = deduplicate(all_cards)
        print(f"\nUnique listings: {len(all_cards)}", flush=True)

        # Phase 2: detail pages
        for idx, job in enumerate(all_cards):
            print(f"  [{idx+1}/{len(all_cards)}] {job['title']} @ {job['company']}", flush=True)
            try:
                details = scrape_job_detail(page, job["job_url"])
                job.update(details)
            except Exception as exc:
                print(f"      SKIPPED: {exc}", flush=True)
            job["scraped_at"] = datetime.now().isoformat()
            time.sleep(0.5)

        browser.close()

    # Save raw LinkedIn data
    payload = {
        "metadata": {
            "scraped_at": datetime.now().isoformat(),
            "source": "LinkedIn Public Job Search",
            "location_filter": LOCATION,
            "experience_filter": "Internship",
            "search_queries": SEARCH_QUERIES,
            "total_results": len(all_cards),
        },
        "jobs": all_cards,
    }
    with open("internships_linkedin.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    fieldnames = list(all_cards[0].keys()) if all_cards else []
    with open("internships_linkedin.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_cards)

    print(f"Saved raw LinkedIn data ({len(all_cards)} listings)", flush=True)
    return all_cards


# ── Merge ───────────────────────────────────────────────────────────

def categorize(job):
    t = (job.get("title", "") + " " + job.get("job_function", "") + " " + job.get("industries", "")).lower()
    if any(w in t for w in ["audio", "sound", "dsp", "signal processing", "acoustic"]):
        return "Sound Engineering"
    if any(w in t for w in ["recording", "studio assistant"]):
        return "Recording Studio"
    if any(w in t for w in ["broadcast", "radio", "news", "espn", "abc7", "wls"]):
        return "Broadcast & Radio"
    if any(w in t for w in ["post-production", "post production", "film", "video", "visual media"]):
        return "Post-Production & Film"
    if any(w in t for w in ["live sound", "live event", "concert", "av ", "audiovisual", "audio visual"]):
        return "Live Sound & Events"
    if any(w in t for w in ["music", "opera", "ravinia"]):
        return "Music Business"
    if any(w in t for w in ["production", "media"]):
        return "Media Production"
    if any(w in t for w in ["marketing", "content", "brand", "ecommerce", "growth"]):
        return "Marketing"
    if any(w in t for w in ["design", "architect", "civil", "engineer", "electrical", "mechanical", "sustainable"]):
        return "Design & Engineering"
    return "Other"


def merge_data(linkedin_jobs):
    """Merge curated listings with LinkedIn data and save."""
    with open("Chicago Audio Internships.json") as f:
        existing = json.load(f)

    # Get curated listings (source == "curated")
    curated = [l for l in existing.get("listings", []) if l.get("source") == "curated"]
    if not curated:
        # First run fallback — treat all existing as curated
        curated = existing.get("listings", [])
        for c in curated:
            c["source"] = "curated"

    # Remove old aggregate placeholders
    curated = [c for c in curated if not any(
        agg in c.get("title", "").lower()
        for agg in ["(8+)", "(43+)", "(116+)", "(multiple)"]
    )]

    # Find duplicates
    dupe_ids = set()
    for j in linkedin_jobs:
        t = j["title"].lower()
        co = j["company"].lower()
        if "espn chicago" in t and "good karma" in co:
            dupe_ids.add(j["job_id"])
        elif "audio news desk" in t and "wbez" in co:
            dupe_ids.add(j["job_id"])
        elif "abc7" in t and ("disney" in co or "walt disney" in co):
            dupe_ids.add(j["job_id"])

    merged = list(curated)
    next_id = max((m.get("id", 0) for m in merged), default=0) + 1

    for j in linkedin_jobs:
        if j.get("job_id") in dupe_ids:
            continue
        # Skip if already in merged by job_url
        if any(m.get("apply_url") == j.get("job_url") for m in merged):
            continue

        cat = categorize(j)
        salary = j.get("salary", "")
        paid = True if salary else None
        rate = salary if salary else None
        emp_type = j.get("employment_type", "")
        if "volunteer" in emp_type.lower() or "unpaid" in j.get("description_text", "").lower()[:200]:
            paid = False
            rate = "Unpaid"

        merged.append({
            "id": next_id,
            "title": j["title"],
            "company": j["company"],
            "category": cat,
            "location": j["location"],
            "paid": paid,
            "rate": rate,
            "hours": "Full-time" if "full" in emp_type.lower() else ("Part-time" if "part" in emp_type.lower() else ""),
            "duration": "",
            "deadline": None,
            "open": True,
            "requirements": [],
            "apply_url": j["job_url"],
            "source": "linkedin",
            "description": j.get("description_text", "")[:500],
            "employment_type": emp_type,
            "seniority_level": j.get("seniority_level", ""),
            "job_function": j.get("job_function", ""),
            "industries": j.get("industries", ""),
            "applicant_count": j.get("applicant_count", ""),
            "company_url": j.get("company_url", ""),
            "company_logo": j.get("company_logo", ""),
            "posting_date": j.get("posting_date", ""),
            "linkedin_id": j.get("job_id", ""),
        })
        next_id += 1

    all_cats = sorted(set(m.get("category", "Other") for m in merged))

    # Save JSON
    output = {
        "title": "Chicago Audio & Recording Internships",
        "scraped": datetime.now().strftime("%Y-%m-%d"),
        "updated": datetime.now().isoformat(),
        "total": len(merged),
        "categories": all_cats,
        "sources": ["curated", "linkedin"],
        "listings": merged,
    }
    with open("Chicago Audio Internships.json", "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    # Save CSV
    if merged:
        fieldnames = list(merged[0].keys())
        with open("Chicago Audio Internships.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in merged:
                row_copy = dict(row)
                if isinstance(row_copy.get("requirements"), list):
                    row_copy["requirements"] = "; ".join(row_copy["requirements"])
                writer.writerow(row_copy)

    print(f"\nMerged total: {len(merged)} listings across {len(all_cats)} categories", flush=True)
    return merged


# ── Main ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"=== Weekly Update: {datetime.now().isoformat()} ===", flush=True)
    linkedin_jobs = run_scraper()
    merge_data(linkedin_jobs)
    print("Done.", flush=True)
