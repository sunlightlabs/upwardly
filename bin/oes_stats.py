from saucebrush import emitters, filters, sources, stats, run_recipe

infile = open('/Users/Jeremy/Downloads/occupations.csv')

std_dev = stats.StandardDeviation('msa_count')
min_max = stats.MinMax('msa_count')
histogram = stats.Histogram('msa_count')

occ_histogram = stats.Histogram("title")
occ_histogram.label_length = 20

run_recipe(
    sources.CSVSource(infile, fieldnames=('id','title','msa_count')),
    filters.FieldModifier('msa_count', int),
    std_dev,
    min_max,
    histogram,
    occ_histogram,
    #emitters.DebugEmitter(),
)

print "Min/Max:", min_max.value()
print "Average:", std_dev.average()
print "Median:", std_dev.median()
print "Std Dev:", std_dev.value()

print histogram
