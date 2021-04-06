from mrjob.job import MRJob
from mrjob.job import MRStep

TOP_YEAR = 2000
BOTTOM_YEAR = 1980


class testing(MRJob):
    def my_mapper(self, _, row):
        row = row.split(",")
        if BOTTOM_YEAR <= int(row[0]) <= TOP_YEAR and row[0] != 'iyear':
            yield (row[9], row[7]), 1

    def my_reducer(self, region, country_count):
        yield region, sum(country_count)

    def my_mapper_final(self, place, max_count):
        region, country = place
        yield region, (country, max_count)

    def my_reducer_final(self, region, country_count):
        temp = list(zip(*list(country_count)))
        index = temp[1].index(max(list(temp[1])))
        yield None, (region, temp[0][index], temp[1][index])

    def sort_res(self, _, results):
        results = sorted(results, key=lambda x: x[2], reverse=True)
        results = [[x, y] for x, y, z in results]
        for region, county in results:
            yield region, county

    def steps(self):
        return [
            MRStep(mapper=self.my_mapper,
                   reducer=self.my_reducer),
            MRStep(mapper=self.my_mapper_final,
                   reducer=self.my_reducer_final),
            MRStep(reducer=self.sort_res)
        ]


if __name__ == '__main__':
    testing.run()
