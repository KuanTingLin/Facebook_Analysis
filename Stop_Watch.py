from datetime import datetime

class StopWatch:
    start = None

    @staticmethod
    def start():
        StopWatch.start = datetime.now()

    @staticmethod
    def split(break_point):
        try:
            split = datetime.now()
            time_elapsed = str(split - StopWatch.start)
            StopWatch.start = split
            print("break point = " + break_point + "time elapsed = " + time_elapsed)
        except:
            print("ValueError: StopWatch.split parameter must be string")