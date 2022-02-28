from datetime import datetime
import sqlite3
from typing import Dict, List
from icalendar import Calendar
import requests
import logging
import os

class UserConfig:
    def __init__(self, **kwargs) -> None:

        self.stop = False

        for key in kwargs:
            try:
                setattr(self, key, kwargs[key])
            except AttributeError:
                continue
        
class User:
    def __init__(self, tg_id: int, ical_address: str, config: UserConfig):
        self.tg_id = tg_id
        self.subscription = ical_address
        self.user_config = config
        self.calendar = None
        pass

    def release_calendar(self):
        self.calendar = None

class Course:
    def __init__(self, course_code: str, course_name: str, course_type: str, start_time: int, end_time: int, user_id: int):
        self.code = course_code
        self.name = course_name
        self.type = course_type
        self.start = start_time
        self.end = end_time
        self.user_id = user_id
        pass

class NotifyDispatcher:
    def __init__(self, database):
        self.db = database
        self.users = {}
        pass

    def add_user(self, user: User):
        if (user.tg_id in self.users):
            return False

        self.users[user.tg_id] = user
        if user.calendar:
            self.dispatchForUser(user.tg_id)
        conn = sqlite3.connect(self.db)
        if conn:
            cur = conn.cursor()
            cur.execute('INSERT INTO User VALUES (?, ?)', (user.tg_id, user.subscription))
            cur.execute('INSERT INTO UserConfig VALUES (?, ?)', (user.tg_id, user.user_config.stop))
            conn.commit()
            conn.close()
            return True
        else:
            return False

    def downloadIcals(self, fetch_local: bool, force_use_local: bool=False):
        conn = sqlite3.connect(self.db)
        if not conn:
            return False
        icals = {}
        cur = conn.cursor()
        cur = cur.execute('SELECT tg_id, ical_address, stop from `User` NATURAL JOIN `UserConfig`')
        i = 0
        for row in cur.fetchall():
            self.users[row[0]] = User(row[0], row[1], UserConfig(stop=bool(row[2])))
            i += 1

        conn.close()

        has_failed = False
        for user_id in self.users:
            user = self.users[user_id]
            ical_path = f'./ical/{user_id}.ics'
            if fetch_local and (force_use_local or os.path.exists(ical_path)):
                    ical_file = open(ical_path, 'r')
                    cal = Calendar.from_ical(ical_file.read())
                    icals[user_id] = cal
                    ical_file.close()
            else:
                response = requests.get(user.subscription)
                if response.ok:
                    ical_file = open(ical_path, 'w')
                    ical_file.write(response.text)
                    ical_file.close()
                    cal = Calendar.from_ical(response.text)
                    icals[user_id] = cal
                else:
                    logging.warning('ical file download failed for: ' + user.subscription + ' , user chat id: '+ user_id)
                    has_failed = True

        if has_failed:
            self.downloadIcals(fetch_local=True, force_use_local=True)
            logging.warning('One or more ical download failed, please check their validation or internet issue. The ical data has not been updated and now using local data.')
        else:
            for id in icals:
                self.users[id].calendar = icals[id]
                
        return True

    def query_course_by_time(self, hour_time: int):
        conn = sqlite3.connect(self.db)
        if not conn:
            return False
        cursor = conn.cursor()
        courses = []
        if type(hour_time) == int and hour_time >= 8 and hour_time <= 21:
            QUERY_COURSE_BY_TIME = "SELECT * from Course WHERE start_time = :hour"
            res = cursor.execute(QUERY_COURSE_BY_TIME, {'hour': hour_time + 1})
            for c_data in res.fetchall():
                courses.append(Course(c_data[0], c_data[1], c_data[2], c_data[3], c_data[4], c_data[5]))
        conn.close()
        return courses

    def __dispatch(self, tg_id):
        sessions = []
        cal = self.users[tg_id].calendar
        events_dat = cal.subcomponents
        for event in events_dat:
            if (event.name == 'VEVENT'):
                desc = str(event['DESCRIPTION'])
                start_time: datetime = event['DTSTART'].dt
                if (start_time.date() != datetime.today().date()):
                    continue
                end_time:datetime = event['DTEND'].dt
                infos = {}
                lns = desc.splitlines()
                for ln in lns:
                    if ln == '':
                        continue
                    parts = ln.split(':')
                    if len(parts) == 2:
                        infos[parts[0]] = parts[1].strip()
                course = (infos['Unit Code'], infos['Unit Description'], infos['Event type'], start_time.hour, end_time.hour, tg_id)
                sessions.append(course)
        self.users[tg_id].release_calendar()
        return sessions

    def dispatchForUser(self, tg_id):
        sessions = self.__dispatch(tg_id)
        conn = sqlite3.connect(self.db)
        cur = conn.cursor()
        cur.executemany("INSERT INTO Course (course_code, course_name, course_type, start_time, end_time, user_id) VALUES (?, ?, ?, ?, ?, ?)", sessions)
        conn.commit()
        conn.close()

    def dispatchAll(self):
        sessions = []
        for user_id in self.users:
            sessions.extend(self.__dispatch(user_id))
        conn = sqlite3.connect(self.db)
        cur = conn.cursor()
        cur.execute("DELETE FROM Course")
        cur.executemany("INSERT INTO Course (course_code, course_name, course_type, start_time, end_time, user_id) VALUES (?, ?, ?, ?, ?, ?)", sessions)
        conn.commit()
        conn.close()
