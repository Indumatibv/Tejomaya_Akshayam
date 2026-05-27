# import requests

# proxy = "socks5://127.0.0.1:9050"  # replace with your proxy
# try:
#     r = requests.get(
#         "https://www.rbi.org.in/",
#         proxies={"http": proxy, "https": proxy},
#         timeout=10
#     )
#     print(f"Status: {r.status_code}, Length: {len(r.text)}")
#     if r.status_code == 200:
#         print("PROXY WORKS for RBI!")
#     else:
#         print("Proxy reached RBI but got blocked")
# except Exception as e:
#     print(f"Proxy failed: {e}")

# ----------------------------------------
# import requests

# try:
#     r = requests.get(
#         "https://www.rbi.org.in/",
#         timeout=10
#     )
#     print(f"Status: {r.status_code}, Length: {len(r.text)}")
#     if r.status_code == 200:
#         print("RBI accessible from this machine!")
#     else:
#         print("Blocked:", r.status_code)
# except Exception as e:
#     print(f"Failed: {e}")


# ----------------------------------------
# import requests

# # Get fresh Indian proxies programmatically
# def get_indian_proxies():
#     try:
#         r = requests.get(
#             "https://proxylist.geonode.com/api/proxy-list?"
#             "limit=10&page=1&sort_by=lastChecked&sort_type=desc"
#             "&country=IN&protocols=http",
#             timeout=10
#         )
#         data = r.json()
#         return [
#             f"http://{p['ip']}:{p['port']}"
#             for p in data.get("data", [])
#         ]
#     except Exception as e:
#         print(f"Could not fetch proxy list: {e}")
#         return []

# proxies_to_test = get_indian_proxies()

# for proxy in proxies_to_test[:5]:
#     try:
#         r = requests.get(
#             "https://www.rbi.org.in/",
#             proxies={"http": proxy, "https": proxy},
#             timeout=8
#         )
#         print(f"✅ {proxy} → Status: {r.status_code}, Len: {len(r.text)}")
#         break
#     except Exception as e:
#         print(f"❌ {proxy} → {e}")

# test_do_you_even_need_proxy.py
import requests

sites = {
    "RBI":          "https://www.rbi.org.in/",
    "SEBI":         "https://www.sebi.gov.in/",
    "MCA":          "https://www.mca.gov.in/",
    "IBBI":         "https://www.ibbi.gov.in/",
    "ICAI":         "https://www.icai.org/",
    "IFSCA":        "https://ifsca.gov.in/",
}

for name, url in sites.items():
    try:
        r = requests.get(url, timeout=10)
        print(f"✅ {name}: {r.status_code} (Len={len(r.text)})")
    except Exception as e:
        print(f"❌ {name}: {e}")