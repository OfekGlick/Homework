from mrjob.job import MRJob
from mrjob.job import MRStep
from datetime import datetime


class testing(MRJob):
    def my_mapper(self, _, row: str):
        row = {x.split(":")[0].strip(r"\""): x.split(":")[1].strip("\"") for x in row[1:len(row)].split(',')}
        date = datetime.fromtimestamp(int(row['Timestamp'][:-1]))
        yield (row['JourneyId'], date.strftime("%m/%d/%Y")), date.time().strftime("%H:%M:%S")

    def my_reducer(self, key, value):
        value = list(map(lambda x: datetime.strptime(x, "%H:%M:%S"), value))
        max_time = max(value)
        min_time = min(value)
        yield key[0], (key[1], int((max_time - min_time).total_seconds()))

    def my_reducer_final(self, key, value):
        value = list(value)
        sum_time = list(zip(*value))[1]
        avg_time = sum(sum_time) / len(value)
        temp = [(date, abs(time - avg_time)) for date, time in value]
        closest_date = min(temp, key=lambda x: x[1])[0]
        sol = [((key, date), travel_time) for date, travel_time in value]
        for var in sol:
            yield var[0], var[1]
        yield key, (avg_time, closest_date)

    def steps(self):
        return [
            MRStep(mapper=self.my_mapper,
                   reducer=self.my_reducer),
            MRStep(reducer=self.my_reducer_final),
        ]


if __name__ == '__main__':
    testing.run()
