#!/usr/bin/python3
"""GitLab CE instance ping to SFDC account processor.

This module is used to process an ever growing list of free GitLab instance
hosts. It identifies information about the organization that owns the domain
and creates an account in SFDC for it. If the account already exists in SFDC,
it updates it with the lastest number of GitLab CE instances and user count.
"""

import re
import psycopg2
import socket
import tldextract
from hosts_to_sfdc.dw_setup import host, username, password, database
from hosts_to_sfdc import discoverorg as dorg
from hosts_to_sfdc import clearbit_gl as cbit
from hosts_to_sfdc import caching
from hosts_to_sfdc import whois_gl

def url_parse(host, ip=None):
    """Return a domain from a url and write to the clean domain cache

    :param host: the hostname to parse
    :return: domain and suffix if parsed or an error string if not.
    """
    # print("Parsing: " + domain)
    result = tldextract.extract(host)
    if result.domain:
        clean_domain = result.domain + '.' + result.suffix
        # Always writing to DB b/c there's no external API request - might as well just update
        caching.write_clean_domain(host, result, clean_domain)
        if ip is not None:
            caching.write_clean_domain(ip, result, clean_domain)
        return clean_domain
    else:
        # Can get an error with http://@#$^#$&*%*sfgdfg@3423
        err = "Not a valid domain"
        return err


def process_domain(domain):
    """Process a domain and update the cache with data if needed.

    This should only take in processed domains.

    Encodes everything in utf-8, as our data is international.
    """
    #TODO probably need their own functions as the data is different. OK for now.
    in_cb_cache = caching.in_cache(domain, 'clearbit_cache')
    in_dorg_cache = caching.in_cache(domain, 'discoverorg_cache')

    if in_cb_cache or in_dorg_cache:
        return

    # Update DiscoverOrg
    written_to_dorg = False
    if not in_dorg_cache:
        written_to_dorg = dorg.update_discoverorg(domain)
        # This will skip Clearbit if we write to Dorg
        if written_to_dorg is True:
            return

    # Update Clearbit
    if not in_cb_cache and written_to_dorg is False:
        cbit.update_clearbit(domain)
        return
    else:
        return


def is_ip(host):
    """
    Returns true if domain is an IP address, otherwise false.
    :param host:
    :return:
    """
    # parsed = urlparse.urlparse(host) # Probably don't need this extra standardization step
    tlded =tldextract.extract(host)
    if len(tlded.ipv4) > 0:
        return True
    else:
        return False


def process_ips(ip_address):
    """Identify a company from an ip address.

    Pulls a list of IP addresses for GitLab hosts and
    cache any data that is found in the data warehouse.
    """
    tlded = tldextract.extract(ip_address)
    tld_ip = tlded.ipv4

    if re.search(r'172\.(1[6-9]|2[0-9]|31)\.|192\.168|10\.', tld_ip):
        # These are reserved for private networks.
        return

    try:
        # Reverse DNS Lookup
        r = socket.gethostbyaddr(tld_ip)
        dns_domain = r[0]
        parsed_domain = url_parse(dns_domain, ip=tld_ip)

        process_domain(parsed_domain)

    except socket.herror:
        # Check WHOIS
        caching.write_clean_domain(raw_domain=ip_address, tldextract=tlded, clean_domain=tld_ip)
        whois_gl.ask_whois(tld_ip)



def url_processor(domain_list):
    """
    Takes a postgres result cursor and iterats through the domains

    :param domain_list: psycopg2 cursor
    :return:pyt
    """
    # print(domain_list.rowcount)
    for url in domain_list:
        the_url = url[0]
        # print(the_url)
        try:
            if is_ip(the_url):
                process_ips(the_url)
            else:
                parsed_domain = url_parse(the_url)
                process_domain(parsed_domain)
        except Exception as e:
            # Skips error
            continue


def process_version_checks():
    mydb = psycopg2.connect(host=host, user=username,
                            password=password, dbname=database)
    cursor = mydb.cursor()

    # Random Sample
    # cursor.execute("SELECT coalesce(hostname, source_ip) as domain FROM version.usage_data TABLESAMPLE SYSTEM_ROWS(75)")

    # Main Query
    cursor.execute("SELECT vc.referer_url "
                   "FROM version.version_checks AS vc "
                     "LEFT JOIN cleaned_urls AS clean ON clean.domain = vc.referer_url "
                   "WHERE vc.updated_at >= (now() - '60 days' :: INTERVAL) "
                     "AND vc.gitlab_version !~ '.*ee' "
                     "AND vc.updated_at > clean.last_update "
                   "GROUP BY vc.referer_url "
                   "UNION "
                   "SELECT coalesce(ud.hostname, ud.source_ip) "
                   "FROM version.usage_data AS ud "
                     "LEFT JOIN cleaned_urls AS clean ON clean.domain = coalesce(ud.hostname, ud.source_ip) "
                   "WHERE ud.updated_at >= (now() - '60 days' :: INTERVAL) "
                     "AND ud.version !~ '.*ee' "
                     "AND ud.updated_at > clean.last_update "
                   "GROUP BY coalesce(ud.hostname, ud.source_ip)")

    url_processor(cursor)

if __name__ == "__main__":
    process_version_checks()
