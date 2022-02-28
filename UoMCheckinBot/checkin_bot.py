import logging
from time import sleep
import schedule

from icalendar import Calendar

from telegram import Update
from telegram.ext import Updater, MessageHandler, Filters, CommandHandler, CallbackContext, ConversationHandler

import threading

from .notify_dispatcher import *

logging.basicConfig(
     format='%(asctime)s %(levelname)-8s %(message)s',
     level=logging.INFO,
     datefmt='%Y-%m-%d %H:%M:%S')

token_file = open('.TOKEN', 'r')
TOKEN = token_file.read()
token_file.close()

HINT_IMAGE_PATH = "./ical_demo_screenshot.png"

END = False

class UoMCheckinBot:
    def __init__(self) -> None:
        self.tg_updater = Updater(TOKEN)
        self.tg_dispatcher = self.tg_updater.dispatcher
        self.notify_dispatcher = NotifyDispatcher('./db/bot-database.sqlite')
        self.hint_image = None
        self.__setup_command_handlers()
        pass

    def run(self):
        schedule.every().minutes.do(self.update)
        schedule.every().day.at("06:00").do(self.dispatchTodaySessions)
        t1 = threading.Thread(target=self.sched_loop)
        t1.start()
        # t2 = threading.Thread(target=self.tg_updater.start_polling())
        # t2.start()
        self.tg_updater.start_polling()

    def sched_loop(self):
        while not END:
            schedule.run_pending()
            sleep(1.0)

    def dispatchTodaySessions(self, fetch_local_icals=False):
        if self.notify_dispatcher.downloadIcals(fetch_local=fetch_local_icals):
            self.notify_dispatcher.dispatchAll()
        else:
            logging.error("A database issue occured when trying to download ical files.")

    def update(self):
        if datetime.now().minute == 50:
            self.__check_and_send_notifies()
            
    def __send_notify(self, chat_id: int, course: Course):
        code = course.code
        unit = course.name
        type = course.type
        msg = f"Hey, you have a {type} session of {unit} ({code}) in 10 minutes, don't forget to check-in here: \nhttps://my.manchester.ac.uk/MyCheckIn"
        self.tg_dispatcher.bot.send_message(chat_id, msg)

    def __check_and_send_notifies(self):
        current_hour = datetime.now().hour
        courses = self.notify_dispatcher.query_course_by_time(current_hour)
        if courses:
            for course in courses:
                self.__send_notify(course.user_id, course)
        else:
            if (current_hour >= 8 and current_hour <= 16):
                logging.info("This is a bit wierd since it's working hour but no courses at this hour was found, there might be a problem, or not.")

    def __start(self, update: Update, context: CallbackContext):
        welcome_msg = "Hi, Welcome to use this bot. If you're a student of UoM, this bot can notify you to check-in for every session! \nLet's not keep missing the check-in, for not get droped-out someday! \nUse /setup to activate this bot for you, and we'll need you to give some of your information."
        context.bot.send_message(chat_id=update.effective_chat.id, text=welcome_msg)

    def __setup(self, update: Update, context: CallbackContext):
        setup_msg = "First, we need your UoM timetable's ical subscription address, which can be found in your https://timetables.manchester.ac.uk/ (*SUBSCRIBE->More->Manual subscription->COPY*), then paste the link here."
        setup_deny_msg = "This bot is currently not supported to be used in a group."
        if (update.effective_chat.id < 0):
            update.message.reply_text(text=setup_deny_msg)
            return ConversationHandler.END
        msg = update.message.reply_text(text=setup_msg)
        if not self.hint_image:
            img = open(HINT_IMAGE_PATH, "rb")
            img_msg = msg.reply_photo(photo=img)
            if img_msg:
                self.hint_image = img_msg.photo[0]
            img.close()
        else:
            msg.reply_photo(photo=self.hint_image)
        context.chat_data['id'] = update.effective_chat.id
        return 0
    
    def __verify_subscription(self, href: str):
        response = requests.get(href)
        if response:
            if response.ok:
                ical = Calendar.from_ical(response.text)
                sample_event = ical.subcomponents[1]
                if sample_event['DESCRIPTION']:
                    desc = str(sample_event['DESCRIPTION'])
                    lines = desc.splitlines()
                    if (len(lines) >= 5):
                        infos = {}
                        for ln in lines:
                            parts = ln.split(': ')
                            if len(parts) == 2:
                                infos[parts[0]] = parts[1]
                        if ('Event type' in infos) and ('Unit Code' in infos) and ('Unit Description' in infos):
                            return ical
        return False

    def __setup_2(self, update: Update, context: CallbackContext):
        VALIDATING_MSG = "Validating your ical subscription url, please wait for a few seconds..."
        SUCCESS_MSG = "Exellent, everythings' done. You will be notified to go check-in by this bot when every session is about to start."
        VERIFY_FAILED_MSG = "Sorry, This link does not seem like a UoM timetable link. there might be internet issues, or you provided a wrong link. If you're sure everything's done right, it could be my problem, you can commit an issue on [GitHub](https://github.com/GrayNekoBean/uom_checkin_alarm_bot) or contact @GrayNekoBean for reporting the bug."
        DB_FAILED_MSG = "Sorry, I think you have already done setup here, or there might be a server side issue. If you think this is a bug, please commit an issue on [GitHub](https://github.com/GrayNekoBean/uom_checkin_alarm_bot) or contact @GrayNekoBean for reporting the bug."
        link = update.message.text
        update.message.reply_text(VALIDATING_MSG)
        verified_ical = self.__verify_subscription(link)
        if verified_ical:
            user = User(context.chat_data['id'], link, UserConfig())
            user.calendar = verified_ical
            if self.notify_dispatcher.add_user(user):
                update.message.reply_text(SUCCESS_MSG)
            else:
                update.message.reply_text(DB_FAILED_MSG)
        else:
            update.message.reply_markdown(VERIFY_FAILED_MSG)
        return ConversationHandler.END

    def __input_valid_url(self, update: Update, context: CallbackContext):
        MSG = "Sorry, the url can't be accepted, please input an valid url"
        update.message.reply_text(MSG)

    def __cancel_setup(self, update: Update, context: CallbackContext):
        MSG = 'setup canceled.'
        update.message.reply_text(MSG)
        return ConversationHandler.END

    def __setup_command_handlers(self):
        self.start_handler = CommandHandler('start', self.__start)
        self.setup_handler = ConversationHandler(
            entry_points= [CommandHandler('setup', self.__setup)],
            states={
                0: [MessageHandler(Filters.regex('^(http:\/\/www\.|https:\/\/www\.|http:\/\/|https:\/\/)?[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,5}(:[0-9]{1,5})?(\/.*)?$'), self.__setup_2),  MessageHandler(~Filters.command, self.__input_valid_url)]
            },
            fallbacks=[CommandHandler('cancel', self.__cancel_setup)]
        )
        self.tg_dispatcher.add_handler(self.start_handler)
        self.tg_dispatcher.add_handler(self.setup_handler)