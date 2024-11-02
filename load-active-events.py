import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random

import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.dialects.postgresql as sqlalchemy_pg
import datetime
import logging

import typing

import os

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

POSTGRES_URL: typing.Final[str] = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@"
    f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/"
    f"{os.getenv('POSTGRES_DB')}?sslmode=require"
)
DT_LOGIN: typing.Final[str] = os.getenv("DR_LOGIN")
DT_PASSWORD: typing.Final[str] = os.getenv("DR_PASSWORD")
BASE_URL: typing.Final[str] = "https://distantrace.com"
LOGIN_URL: typing.Final[str] = "/lv/konts/login/"

class Base(sqlalchemy.orm.DeclarativeBase):
    pass

class Events(Base):
    __tablename__ = "events"
    id: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(sqlalchemy.VARCHAR(32), primary_key=True)
    name: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(sqlalchemy.String)

    def __repr__(self) -> str:
        return f"Event(id={self.id}, name={self.name})"

class Participants(Base):
    __tablename__ = "participants"
    id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.BigInteger, primary_key=True)
    name: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(sqlalchemy.String)

    def __repr__(self) -> str:
        return f"Participant(id={self.id}, name={self.name})"

class Results(Base):
    __tablename__ = "results"
    event_id: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(sqlalchemy.VARCHAR(32), sqlalchemy.ForeignKey("events.id"), primary_key=True)
    participant_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.BigInteger, sqlalchemy.ForeignKey("participants.id"), primary_key=True)
    result_date: sqlalchemy.orm.Mapped[datetime.date] = sqlalchemy.orm.mapped_column(sqlalchemy.Date, primary_key=True)
    distance: sqlalchemy.orm.Mapped[float] = sqlalchemy.orm.mapped_column(sqlalchemy.NUMERIC)
    steps: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.Integer)

    def __repr__(self) -> str:
        return f"Result(event_id={self.event_id}, participant_id={self.participant_id}, result_date={self.result_date}, distance={self.distance}, time={self.time})"

def random_sleep(min: float = 1, max: float = 10) -> None:
    sleep_time = random.uniform(min, max)
    logger.debug(f"Sleeping for {sleep_time:.2f} seconds")
    time.sleep(sleep_time)

def login() -> tuple[requests.Session, requests.Response]:
    session = requests.Session()
    login_page = session.get(BASE_URL + LOGIN_URL, headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0"})
    csfr_token = BeautifulSoup(login_page.text, "html.parser").find("input", {"name": "csrfmiddlewaretoken"})["value"]
    login_data = {
        "csrfmiddlewaretoken": [csfr_token, csfr_token],
        "login": DT_LOGIN,
        "password": DT_PASSWORD,
    }
    random_sleep()
    login_response = session.post(BASE_URL + LOGIN_URL, data=login_data, headers={
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0",
        "Referer": BASE_URL + LOGIN_URL
    })
    return session, login_response

def get_active_event(session: requests.Session) -> tuple[str, str, list[str]]:
    session, login_response = login()
    parsed_login_response = BeautifulSoup(login_response.text, "html.parser")
    active_events = [h5.find("a")["href"] for h5 in parsed_login_response.find("div", {"id": "pills-active-events"}).find_all("h5", {"class": "card-title"})]
    if not active_events:
        return []
    random_sleep()
    active_event_page = session.get(BASE_URL + active_events[0] + "dalibnieki/", headers={
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0",
        "Referer": BASE_URL + active_events[0]
    })
    parsed_active_event_page = BeautifulSoup(active_event_page.text, "html.parser")
    event_name = parsed_active_event_page.find("h1").text
    event_id = active_events[0].split("/")[-2]
    logger.info("Event name: %s", event_name)
    participants = [a["href"] for a in parsed_active_event_page.find("div", {"class": "table-container"}).find("table").find("tbody").find_all("a") if "dalibnieki" in a["href"]]
    return event_id, event_name, participants

def get_participant_data(session: requests.Session, participant_url: str) -> pd.DataFrame:
    logger.info("Processing participant %s", participant_url)
    random_sleep()
    participant_response = session.get(
        BASE_URL + participant_url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0",
            "Referer": BASE_URL + participant_url
        }
    )
    participant_parsed = BeautifulSoup(participant_response.text, "html.parser")
    name = participant_parsed.find("h3", {"class": "text-secondary"}).text
    logger.info("Participant name: %s", name)
    id = participant_url.split("/")[-2]
    results = pd.DataFrame(
        [{
            "participant_id": id,
            "participant_name": name,
            "result_date": pd.to_datetime(tr[1], dayfirst=True).date(),
            "distance": float(tr[2].replace(",", ".")),
            "steps": int(tr[3].replace(",", ""))
        } for tr in [[td.text for td in tr.find_all("td")] for tr in participant_parsed.find("div", {"class": "table-container"}).find("table").find("tbody").find_all("tr")]]
    )
    next_page_url = [a["href"] for a in participant_parsed.find("nav", {"class": "pagination"}).find_all("a") if not a["href"].startswith("#")]
    if next_page_url:
        logger.info("Next page URL: %s", next_page_url[-1])
        random_sleep()
        next_page_response = requests.get(
            BASE_URL + participant_url + next_page_url[-1],
            headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0"},
            cookies=session.cookies
        )
        next_page_parsed = BeautifulSoup(next_page_response.text, "html.parser")
        next_page_data = pd.DataFrame(
            [{
                "participant_id": id,
                "participant_name": name,
                "result_date": pd.to_datetime(tr[1], dayfirst=True).date(),
                "distance": float(tr[2].replace(",", ".")),
                "steps": int(tr[3].replace(",", ""))
            } for tr in [[td.text for td in tr.find_all("td")] for tr in next_page_parsed.find("div", {"class": "table-container"}).find("table").find("tbody").find_all("tr")]]
        )
        results = pd.concat([results, next_page_data])
    return results

def get_all_data(session: requests.Session) -> pd.DataFrame:
    event_id, event_name, participants = get_active_event(session)
    results = pd.DataFrame()
    for participant in participants:
        results = pd.concat([results, get_participant_data(session, participant)])
    results["event_id"] = event_id
    results["event_name"] = event_name
    return results

def write_to_db(results: pd.DataFrame) -> None:
    engine = sqlalchemy.create_engine(POSTGRES_URL, echo=True)
    Base.metadata.create_all(engine)
    # Merge event
    events = results[["event_id", "event_name"]].drop_duplicates().rename(columns={"event_id": "id", "event_name": "name"})
    with sqlalchemy.orm.Session(engine) as session:
        for _, row in events.iterrows():
            session.merge(Events(**row))
        session.commit()
    # Merge participants
    participants = results[["participant_id", "participant_name"]].drop_duplicates().rename(columns={"participant_id": "id", "participant_name": "name"})
    with sqlalchemy.orm.Session(engine) as session:
        for _, row in participants.iterrows():
            session.merge(Participants(**row))
        session.commit()
    # Merge results
    with sqlalchemy.orm.Session(engine) as session:
        for _, row in results.drop(["participant_name", "event_name"], axis=1).iterrows():
            session.merge(Results(**row))
        session.commit()

def main() -> None:
    session = requests.Session()
    results = get_all_data(session)
    write_to_db(results)

if __name__ == "__main__":
    main()
