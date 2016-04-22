# http://blog.mague.com/?p=201
set title "SITE.com Web Traffic"
set xlabel "Time"
set ylabel "temperature"
set xdata time
set timefmt "%s"
set format x "%m/%d - %H:%M:%S"
set key left top
set grid
plot "/Users/mborges/Downloads/temperature.csv" using 1:2 with lines lw 2 lt 3 title "temperature"

