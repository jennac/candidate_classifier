from argparse import ArgumentParser
from csv import DictReader, DictWriter


def get_args():
    parser = ArgumentParser(description='specify which type of social')

    parser.add_argument('-t', '--twitter',
                        help='finds twitter links',
                        action='store_true')

    parser.add_argument('-f', '--facebook',
                        help='find facebook links',
                        action='store_true')

    parser.add_argument('-w', '--web',
                        help='find website',
                        action='store_true')

    parser.add_argument('-y', '--youtube', 
                        help='find youtube',
                        action='store_true')

    return parser.parse_args()


def read_data(infile):
    fields = []
    data = []

    with open(infile, 'r') as infile:
        reader = DictReader(infile)
        fields = reader.fieldnames
        data = [row for row in reader]

    return fields, data


def write_data(outfile, data, fields):

    with open(outfile, 'w') as outfile:
        writer = DictWriter(outfile, fieldnames=fields)
        writer.writeheader()
        for d in data:
            writer.writerow(d)


def main():
    args = get_args()
    path = '/Users/jcolazzi/Dropbox/BIP Production/candidates/reports/'
    infile = 'all_states.csv'

    fields, data = read_data(path+infile)

    if args.twitter:
        social_field = 'Twitter Name'
        outfile = 'twitter_training.csv'
    elif args.facebook:
        social_field = 'Facebook URL'
        outfile = 'facebook_training.csv'
    elif args.web:
        social_field = 'Website'
        outfile = 'website_training.csv'
    elif args.youtube:
        social_field = 'Youtube'
        outfile = 'youtube_training.csv'
    else:
        social_field = ''
        outfile = ''

    has_social = [d for d in data if len(d[social_field]) > 0]
    print 'NUMBER OF CANDIDATES WITH SOCIAL: {}'.format(len(has_social))
    write_data(path+outfile, has_social, fields)


if __name__ == '__main__':
    main()
