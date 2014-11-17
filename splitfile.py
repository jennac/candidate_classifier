from argparse import ArgumentParser
from csv import DictReader, DictWriter


def get_args():

    parser = ArgumentParser(description='splits infile into n smaller chunks')

    parser.add_argument('-f', '--filename',
                        help='file to import and split')

    parser.add_argument('-n', '--number',
                        help='number of output files to split into')

    parser.add_argument('-p', '--path',
                        help='path to file')

    return parser.parse_args()


def read_file(infile):

    with open(infile, 'rU') as f:
        reader = DictReader(f)
        fields = reader.fieldnames
        data = [row for row in reader]

    return fields, data


def write_file(outfile, fields, data):

    with open(outfile, 'w') as f:
        writer = DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for d in data:
            writer.writerow(d)


def split():

    args = get_args()
    split_num = int(args.number)
    infile = args.filename
    if args.path:
        path = args.path
    else:
        path = ''

    # path = '/Users/jcolazzi/Dropbox/BIP Production/candidates/reports/social/'
    # path = '/Users/jcolazzi/bip/candidate_classifier/web/srsplit/'
    path = '/Users/jcolazzi/bip/candidate_classifier/twitter/srsplit/'

    fields, data = read_file(path+infile)

    # x and y are indices for the m sized split files
    # R is the remainder and will be tacked on the final chunk
    m = len(data) / split_num
    R = len(data) % split_num
    x = 0
    y = x + m

    print 'SPLITS WILL BE LEN {}'.format(m)
    for i in range(split_num):
        outfile = 'SPLIT_{}_{}'.format(i, infile)
        write_file(path+outfile, fields, data[x:y])
        x = y
        y = x + m
        if i == (split_num - 2):
            y += R


if __name__ == '__main__':
    split()
