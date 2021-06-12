import json, csv, os


def flatten_json(jsonfile):
    out = {}

    def flatten(x, name=''):
        if isinstance(x, dict):
            for value in x:
                flatten(x[value], name + value + '.')
        elif isinstance(x, list):
            i = 0
            for value in x:
                flatten(value, name + str(i) + '.')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(jsonfile)
    return out


def json_to_TSV(jsonfile, headers):
    with open('converted.tsv', 'w') as bla:
        filewriter = csv.writer(bla, delimiter="\t")
        filewriter.writerow(headers)
        for row in jsonfile:
            temp = row
            for header in headers:
                if header not in row.keys():
                    temp[header] = ""
            temp = sorted(temp.items())
            final_row = []
            for key, value in temp:
                final_row.append(value)
            filewriter.writerow(final_row)


def main():
    with open('pokedex.json') as jsonfile:
        data = json.load(jsonfile)
        data = list(map(flatten_json, data))
        headers = list(map(sorted, data))
        headers = [header for value in headers for header in value]
        headers = sorted(list(dict.fromkeys(headers)))
        json_to_TSV(data, headers)


if __name__ == '__main__':
    main()
