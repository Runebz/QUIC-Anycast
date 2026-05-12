for s in 32 64 1K 16K 1M 16M 64M; do
	fallocate -l $s files/$s
done
