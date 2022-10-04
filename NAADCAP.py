#!/usr/bin/env python3

from json import dumps, loads
from urllib.error import HTTPError
from xml.etree.ElementTree import fromstring
from socket import socket, AF_INET, SOCK_STREAM
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from requests import get
from os.path import exists
from os import remove

referenceList = []
refListLock = Lock()


def recv_sock_data(s, num) -> str:
    try:
        data = b""
        buffer = 16384
        reading = False
        while True:
            read = s.recv(buffer)
            if not reading:
                print(f"[SERVER{str(num)}] Data Start")
                reading = True
            data += read
            if len(read) < buffer:
                if "</alert>" in read.decode("utf-8"):
                    print(f"[SERVER{str(num)}] Data End.")
                    break
        return data.decode("utf-8")
    except KeyboardInterrupt:
        exit(0)
    except Exception as E:
        print(f"[SERVER{str(num)}] Exception: {E}")


def saveAlert(ref, data, num):
    try:
        id = ref.split(",")[1]
        with open("NAADFeed.json", "r+") as f:
            oldAlerts = loads(f.read())
            oldAlerts["NAADAlerts"].append(ref)
            f.seek(0)
            f.write(dumps(oldAlerts, indent=4))
            f.truncate()
        if not exists(f"{id}.xml"):
            with open(f"{id}.xml", "w") as f:
                f.write(data)
    except KeyboardInterrupt:
        exit(0)
    except Exception as E:
        print(f"[SERVER{str(num)}] Exception: {E}")


def removeAlert(ref, num):
    try:
        id = ref.split(",")[1]
        with open("NAADFeed.json", "r+") as f:
            oldAlerts = loads(f.read())
            oldAlerts["NAADAlerts"].remove(ref)
            f.seek(0)
            f.write(dumps(oldAlerts, indent=4))
            f.truncate()
        if exists(f"{id}.xml"):
            remove(f"{id}.xml")
    except KeyboardInterrupt:
        exit(0)
    except Exception as E:
        print(f"[SERVER{str(num)}] Exception: {E}")


def main1(server, port, capcp, num):
    try:
        global referenceList
        HOST = server
        PORT = port
        CAPCP = capcp
        ns = {"alert": "urn:oasis:names:tc:emergency:cap:1.2"}
        with socket(AF_INET, SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            while True:
                data = recv_sock_data(s, num)
                NAADCAP = fromstring(data)
                identifier = NAADCAP.find("alert:identifier", ns).text
                sender = NAADCAP.find("alert:sender", ns).text
                sent = NAADCAP.find("alert:sent", ns).text
                if sender == "NAADS-Heartbeat":
                    ref = NAADCAP.find("alert:references", ns).text.split(" ")
                    print(f"[SERVER{str(num)}] NAAD Heartbeat: {identifier}")
                    print(f"[SERVER{str(num)}]  - Sent by: {sender}")
                    print(f"[SERVER{str(num)}]  - Sent at: {sent}")
                    with refListLock:
                        for r in ref:
                            if r not in referenceList:
                                sdr, id, recv = r.split(",")
                                alertURL = f"http://{CAPCP}/{recv.split('T')[0]}/{recv.replace('-', '_').replace(':', '_').replace('+', 'p')}I{id.replace('-', '_').replace(':', '_').replace('+', 'p')}.xml"
                                print(
                                    f"[SERVER{str(num)}]  - New Alert Reference: {id}"
                                )
                                print(f"[SERVER{str(num)}]     - Sent by {sdr}")
                                print(f"[SERVER{str(num)}]     - Sent at {recv}")
                                print(f"[SERVER{str(num)}]     - URL: {alertURL}")
                                alert = get(
                                    url=alertURL,
                                    timeout=10,
                                    headers={"User-agent": "Mozilla/5.0"},
                                )
                                try:
                                    alert.raise_for_status()
                                    print(
                                        f"[SERVER{str(num)}]     - Alert XML Saved to file"
                                    )
                                except HTTPError:
                                    print(
                                        f"[SERVER{str(num)}]     - Failed to fetch XML"
                                    )
                                referenceList.append(r)
                                saveAlert(r, alert.text, num)
                        for l in referenceList:
                            if l not in ref:
                                referenceList.remove(l)
                                removeAlert(l, num)
                                print(f"[SERVER{str(num)}] Removed old Reference ID.")
                else:
                    print(f"[SERVER{str(num)}] NAAD Alert: {identifier}")
                    print(f"[SERVER{str(num)}]  - Sent by: {sender}")
                    print(f"[SERVER{str(num)}]  - Sent at: {sent}")
                    if not f"{sender},{identifier},{sent}" in referenceList:
                        with refListLock:
                            referenceList.append(f"{sender},{identifier},{sent}")
                            saveAlert(f"{sender},{identifier},{sent}", data, num)
                        print(f"[SERVER{str(num)}]  - Alert XML Saved to file")
                    else:
                        print(f"[SERVER{str(num)}]  - Alert already recieved")
                print("\n")
    except KeyboardInterrupt:
        exit(0)
    except Exception as E:
        print(f"[SERVER{str(num)}] Exception: {E}")


def main():
    try:
        global referenceList
        try:
            with open("NAADFeed.json", "r") as f:
                old = loads(f.read())
                for i in old["NAADAlerts"]:
                    referenceList.append(i)
        except FileNotFoundError:
            with open("NAADFeed.json", "w") as f:
                f.write('{"NAADAlerts": []}')
        servers = [
            [
                "streaming1.naad-adna.pelmorex.com",
                8080,
                "capcp1.naad-adna.pelmorex.com",
            ],
            [
                "streaming2.naad-adna.pelmorex.com",
                8080,
                "capcp2.naad-adna.pelmorex.com",
            ],
        ]
        with ThreadPoolExecutor(
            max_workers=len(servers), thread_name_prefix="SERVER"
        ) as executor:
            for i in servers:
                executor.submit(
                    main1,
                    str(i[0]),
                    int(i[1]),
                    str(i[2]),
                    str(servers.index(i) + 1),
                )
    except KeyboardInterrupt:
        exit(0)
    except Exception as E:
        print(f"[MAIN] Exception: {E}")


while __name__ == "__main__":
    main()
