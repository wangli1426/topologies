import storm

class ProcessingBolt(storm.BasicBolt):
    def process(self, tup):
        words = tup.values[0].split(" ")
        station = "123"
        time = "2015-10-26,14:32"
        for word in words:
          storm.emit([station,time,word])

ProcessingBolt().run()