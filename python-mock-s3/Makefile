start:
	python -m mock_s3.main

profile-start:
	python -m cProfile -o profile.out -m mock_s3.main

profile-view:
	# pip install pyprof2calltree
	# sudo apt-get install kcachegrind
	pyprof2calltree -i profile.out -k


deps:
	pip install -r requirements.txt

db:
	docker run -it scylladb/scylla --smp 1