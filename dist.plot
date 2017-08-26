set logscale xy
set xlabel "Rank"
set ylabel "Number of flights"
set terminal png
set output "dist.png"
plot "airport-popularity-rank-vs-count.txt" using 1:2
