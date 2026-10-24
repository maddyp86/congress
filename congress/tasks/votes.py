from congress.tasks import utils
import json
from iso8601 import iso8601
import datetime
import os
import os.path
import re
import urllib.parse
import time
import datetime
from lxml import html, etree
import logging

from congress.tasks import vote_info


def run(options):
    # Parse the options and fill to_fetch with vote ids.

    vote_id = options.get('vote_id', None)
    if vote_id:
        # Just fetch the specified vote.
        vote_chamber, vote_number, congress, session_year = utils.split_vote_id(vote_id)
        sessions = [(congress, session_year)]
        to_fetch = [vote_id]
    else:
        if options.get('congress', None):
            if options.get('session', None):
                # Fetch for one session given with the congress and session options.
                sessions = [(options.get('congress'), options.get('session'))]
            else:
                # Fetch for both sessions of a Congress except sessions that are in the future.
                sessions = []
                first_session_year = utils.get_congress_first_year(options.get('congress'))
                for y in (first_session_year, first_session_year + 1):
                    if y > datetime.datetime.now().year: continue # this session hasn't started yet
                    sessions.append( (options.get('congress'), str(y)) )
        elif options.get('sessions', None):
            # Fetch for multiple sessions, e.g. sessions=117.2021,117.2022
            sessions = [session.split('.') for session in options.get('sessions').split(',')]
        else:
            # Fetch for the current session.
            sessions = [(utils.current_congress(), options.get('session', str(utils.current_legislative_year())))]

        # Get the list of votes.
        to_fetch = []
        index_failures = []
        for congress, session_year in sessions:
            if options.get('chamber', None) in ("house", None):
                ids = vote_ids_for_house(congress, session_year, options)
                if ids is None:
                    index_failures.append("house %s.%s" % (congress, session_year))
                to_fetch += ids or []
            if options.get('chamber', None) in ("senate", None):
                ids = vote_ids_for_senate(congress, session_year, options)
                if ids is None:
                    index_failures.append("senate %s.%s" % (congress, session_year))
                to_fetch += ids or []

        if index_failures:
            # Never report success when a chamber's vote list couldn't be read.
            # Previously this path logged errors and exited 0, so the workflow
            # only failed later with a misleading "0 files" message.
            raise Exception("Could not read vote lists for: %s" % ", ".join(index_failures))

        if not to_fetch:
            if not options.get("fast", False):
                logging.error("Error figuring out which votes to download, aborting.")
            else:
                logging.warn("No new or recent votes.")
            return None

        limit = options.get('limit', None)
        if limit:
            to_fetch = to_fetch[:int(limit)]

    if options.get('pages_only', False):
        return None

    logging.warn("Going to fetch %i votes from congress/session %s" % (len(to_fetch), ", ".join(str(cs) for cs in sessions)))

    utils.process_set(to_fetch, vote_info.fetch_vote, options)

# page through listing of House votes of a particular congress and session


def vote_ids_for_house(congress, session_year, options):
    vote_ids = []

    index_page = "https://clerk.house.gov/evs/%s/index.asp" % session_year
    group_page = r"ROLL_(\d+)\.asp"
    link_pattern = r"http://clerk.house.gov/cgi-bin/vote.asp\?year=%s&rollnumber=(\d+)" % session_year

    # download index page, find the matching links to the paged listing of votes
    page = utils.download(
        index_page,
        "%s/votes/%s/pages/house.html" % (congress, session_year),
        options)

    if not page:
        # The Clerk retired the /evs/<year>/index.asp + ROLL_xxx.asp listing pages
        # (404 since ~Oct 23, 2026), but the per-vote XML files at
        # /evs/<year>/rollNNN.xml are still published and numbered contiguously.
        # Fall back to discovering votes by walking those files.
        logging.warn("House vote index page unavailable; discovering votes by probing roll XML files")
        return vote_ids_for_house_by_probe(congress, session_year, options)

    # extract matching links
    doc = html.document_fromstring(page)
    links = doc.xpath(
        "//a[re:match(@href, '%s')]" % group_page,
        namespaces={"re": "http://exslt.org/regular-expressions"})

    for link in links:
        # get some identifier for this inside page for caching
        grp = re.match(group_page, link.get("href")).group(1)

        # download inside page, find the matching links
        page = utils.download(
            urllib.parse.urljoin(index_page, link.get("href")),
            "%s/votes/%s/pages/house_%s.html" % (congress, session_year, grp),
            options)

        if not page:
            logging.error("Couldn't download House vote group page (%s), aborting" % grp)
            continue

        doc = html.document_fromstring(page)
        votelinks = doc.xpath(
            "//a[re:match(@href, '%s')]" % link_pattern,
            namespaces={"re": "http://exslt.org/regular-expressions"})

        for votelink in votelinks:
            num = re.match(link_pattern, votelink.get("href")).group(1)
            vote_id = "h" + num + "-" + str(congress) + "." + session_year
            if not should_process(vote_id, options):
                continue
            vote_ids.append(vote_id)

    return utils.uniq(vote_ids)


def vote_ids_for_house_by_probe(congress, session_year, options):
    # Walk https://clerk.house.gov/evs/<year>/rollNNN.xml from 1 upward until
    # several consecutive numbers are missing. Each file is saved to the same
    # cache path vote_info.fetch_vote() uses, so the later fetch reads it from
    # cache instead of downloading it a second time.
    max_misses = int(options.get("house_probe_misses", 3))
    vote_ids = []
    misses = 0
    num = 0
    while misses < max_misses:
        num += 1
        url = "https://clerk.house.gov/evs/%s/roll%03d.xml" % (session_year, num)
        body = utils.download(
            url,
            "%s/votes/%s/h%d/h%d.xml" % (congress, session_year, num, num),
            utils.merge(options, {'binary': True}),
        )
        if not body or b"<rollcall-vote" not in body:
            misses += 1
            continue
        misses = 0
        vote_id = "h%d-%s.%s" % (num, congress, session_year)
        if should_process(vote_id, options):
            vote_ids.append(vote_id)

    logging.warn("House %s: found %d roll call votes by probing" % (session_year, num - max_misses))
    if num - max_misses == 0:
        logging.error("No House roll call XML found for %s" % session_year)
        return None
    return vote_ids


def vote_ids_for_senate(congress, session_year, options):
    session_num = int(session_year) - utils.get_congress_first_year(int(congress)) + 1

    vote_ids = []

    url = "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_%s_%d.xml" % (congress, session_num)
    page = utils.download(
        url,
        "%s/votes/%s/pages/senate.xml" % (congress, session_year),
        utils.merge(options, {'binary': True})
    )

    if not page or b"Requested Page Not Found (404)" in page:
        logging.error("Couldn't download Senate vote XML index %s, skipping" % url)
        return None

    dom = etree.fromstring(page)

    # Sanity checks.
    if int(congress) != int(dom.xpath("congress")[0].text):
        logging.error("Senate vote XML returns the wrong Congress: %s" % dom.xpath("congress")[0].text)
        return None
    if int(session_year) != int(dom.xpath("congress_year")[0].text):
        logging.error("Senate vote XML returns the wrong session: %s" % dom.xpath("congress_year")[0].text)
        return None

    # Get vote list.
    for vote in dom.xpath("//vote"):
        num = int(vote.xpath("vote_number")[0].text)
        vote_id = "s" + str(num) + "-" + str(congress) + "." + session_year
        if not should_process(vote_id, options):
            continue
        vote_ids.append(vote_id)
    return vote_ids


def should_process(vote_id, options):
    if not options.get("fast", False):
        return True

    # If --fast is used, only download new votes or votes taken in the last
    # three days (when most vote changes and corrections should occur).
    f = vote_info.output_for_vote(vote_id, "json")
    if not os.path.exists(f):
        return True

    v = json.load(open(f))
    now = utils.eastern_time_zone.localize(datetime.datetime.now())
    return (now - iso8601.parse_date(v["date"])) < datetime.timedelta(days=3)
