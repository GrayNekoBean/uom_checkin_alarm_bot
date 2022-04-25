from datetime import datetime
from distutils.log import info
import sqlite3
from icalendar import Calendar
import requests
import logging
import os
import logging

logger = logging.getLogger('checkin-bot')

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
        self.config = config
        self.calendar = None
        self.ical_content = ''
        pass

    def release_calendar(self):
        self.calendar = None
        self.ical_content = ''

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
            ical_file = open(f'./ical/{user.tg_id}.ics', 'w')
            ical_file.write(user.ical_content)
            ical_file.close()
            self.dispatchForUser(user.tg_id)
        conn = sqlite3.connect(self.db)
        if conn:
            cur = conn.cursor()
            cur.execute('INSERT INTO User VALUES (?, ?)', (user.tg_id, user.subscription))
            cur.execute('INSERT INTO UserConfig VALUES (?, ?)', (user.tg_id, user.config.stop))
            conn.commit()
            conn.close()
            return True
        else:
            return False

    def is_user_exists(self, tg_id: int) -> bool:
        return (tg_id in self.users)

    def is_user_stop_notify(self, tg_id: int) -> bool:
        return self.users[tg_id].config.stop

    def set_user_stop(self, tg_id: int):
        if self.is_user_exists(tg_id):
            conn = sqlite3.connect(self.db)
            if conn:
                self.users[tg_id].config.stop = True
                cur = conn.cursor()
                cur.execute("UPDATE UserConfig SET stop = 1 WHERE tg_id = ?", [tg_id])
                conn.commit()
                conn.close()
                return True
            else:
                logger.error('Database connection failed when setting user config "stop" to true.')
        return False

    def set_user_resume(self, tg_id: int):
        if self.is_user_exists(tg_id):
            conn = sqlite3.connect(self.db)
            if conn:
                self.users[tg_id].config.stop = False
                cur = conn.cursor()
                cur.execute("UPDATE UserConfig SET stop=0 WHERE tg_id=?", [tg_id])
                conn.commit()
                res = cur.execute("SELECT * FROM Course WHERE user_id=?", [tg_id])
                if len(res.fetchall()) == 0:
                    conn.close()
                    self.load_user_calendar(self.users[tg_id])
                    self.dispatchForUser(tg_id)
                return True
            else:
                logger.error('Database connection failed when setting user config "stop" to false.')
        return False

    def update_user_subscription(self, tg_id: int, new_sub: str):
        if self.is_user_exists(tg_id):
            conn = sqlite3.connect(self.db)
            if conn:
                self.users[tg_id].config.stop = False
                cur = conn.cursor()
                cur.execute("UPDATE User SET subscription=? WHERE tg_id=?", [new_sub, tg_id])
                cur.execute("DELETE FROM Course WHERE user_id=?", [tg_id])
                conn.commit()
                conn.close()
                self.load_user_calendar(self.users[tg_id])
                self.dispatchForUser(tg_id)
                return True
            else:
                logger.error('Database connection failed when updating user ical subscription.')
        return False

    def load_user_calendar(self, user: User):
        ical_path = f'./ical/{user.tg_id}.ics'
        response = requests.get(user.subscription)
        if response.ok:
            ical_file = open(ical_path, 'w')
            ical_file.write(response.text)
            ical_file.close()
            cal = Calendar.from_ical(response.text)
            user.calendar = cal
            return True
        else:
            logger.warning('ical file download failed for: ' + user.subscription + ' , user chat id: '+ user.tg_id)
            return False

    # @params
    # fetch_local: when true, this function will tries to fetch from cached local ical files first, if there's any missing ical files, it will still downlaod it from subscription. Otherwise it will download every ical file from subscription and update the whole cached ical file data.
    # force_user_local: only works when fetch_local is true, if it toggles, function will only use local ical files and not going to download any missing ical files. This is only used as a fallback option.

    def load_all_users_calendars(self, fetch_local: bool, force_use_local: bool=False) -> bool:
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
            if user.config.stop:
                continue
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
                    logger.warning('ical file download failed for: ' + user.subscription + ' , user chat id: '+ user_id)
                    has_failed = True

        if has_failed:
            self.load_all_users_calendars(fetch_local=True, force_use_local=True)
            logger.warning('One or more ical download failed, please check their validation or internet issue. The ical data has not been updated and now using local data.')
        else:
            for id in icals:
                if self.users[id].config.stop:
                    continue
                self.users[id].calendar = icals[id]
                
        return True

    def query_course_by_time(self, hour_time: int) -> list:
        conn = sqlite3.connect(self.db)
        if not conn:
            return False
        cursor = conn.cursor()
        courses = []
        if type(hour_time) == int and hour_time >= 7 and hour_time <= 21:
            QUERY_COURSE_BY_TIME = "SELECT * from Course WHERE start_time = :hour"
            res = cursor.execute(QUERY_COURSE_BY_TIME, {'hour': hour_time + 1})
            for c_data in res.fetchall():
                courses.append(Course(c_data[0], c_data[1], c_data[2], c_data[3], c_data[4], c_data[5]))
        conn.close()
        return courses

    def __dispatch(self, tg_id: int) -> list:
        sessions = []
        cal = self.users[tg_id].calendar
        if not cal:
            return []
        events_dat = cal.subcomponents
        today_date = datetime.today().date()
        for event in events_dat:
            if (event.name == 'VEVENT'):
                desc = str(event['DESCRIPTION'])
                start_time: datetime = event['DTSTART'].dt
                if (start_time.date() != today_date):
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
                
                unit_code = 'UNKNOW'
                unit_desc = 'Unknow'
                event_type = 'Unknow type'
                if 'Unit Code' in infos:
                    unit_code = infos['Unit Code']
                elif 'Code' in infos:
                    unit_code = infos['Code']
                else:
                    logger.error('Cannot find "Unit Code" in calendar info. Please check if there is any change on calendar format.')

                if 'Description' in infos:
                    unit_desc = infos['Description']
                elif 'Unit Description' in infos:
                    unit_desc = infos['Unit Description']
                else:
                    logger.error('Cannot find "Description" in calendar info. Please check if there is any change on calendar format.')
                
                if 'Event type' in infos:
                    event_type = infos['Event type']
                else:
                    logger.error('Cannot find "Event type" in calendar info. Please check if there is any change on calendar format.')
                course = (unit_code, unit_desc, event_type, start_time.hour, end_time.hour, tg_id)
                sessions.append(course)
        self.users[tg_id].release_calendar()
        return sessions

    def dispatchForUser(self, tg_id: int):
        sessions = self.__dispatch(tg_id)
        conn = sqlite3.connect(self.db)
        cur = conn.cursor()
        cur.executemany("INSERT INTO Course (course_code, course_name, course_type, start_time, end_time, user_id) VALUES (?, ?, ?, ?, ?, ?)", sessions)
        conn.commit()
        conn.close()

    def dispatchAll(self):
        sessions = []
        n = 0
        f = 0
        for user_id in self.users:
            sess = self.__dispatch(user_id)
            if len(sess) > 0:
                sessions.extend(sess)
                n += 1
            else:
                f += 1
        conn = sqlite3.connect(self.db)
        cur = conn.cursor()
        cur.execute("DELETE FROM Course")
        cur.executemany("INSERT INTO Course (course_code, course_name, course_type, start_time, end_time, user_id) VALUES (?, ?, ?, ?, ?, ?)", sessions)
        conn.commit()
        conn.close()
        logger.info(f'Successfully dispatched today\'s timetable for {n} users with {f} user(s) failed to dispatch.')
