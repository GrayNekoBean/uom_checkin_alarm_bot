from UoMCheckinBot.checkin_bot import *

if __name__ == '__main__':
    bot = UoMCheckinBot()
    bot.dispatchTodaySessions(True)
    bot.run()
