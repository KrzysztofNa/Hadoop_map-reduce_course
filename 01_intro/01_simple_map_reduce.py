from mrjob.job import MRJob

class MRWordCount(MRJob):

    def mapper(self, _, line): # jeżeli pracujemy na txt, to nie potrzeba klucza, dajemy zmenną zastępczą "_".
        yield 'chars', len(line)  # generator zwracający długosc linii (liczba znaków).
        yield 'words', len(line.split(' '))

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRWordCount.run()