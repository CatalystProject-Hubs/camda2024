# virtual environment
.venv/.success:
	python3.10 -m venv .venv
	. .venv/bin/activate && pip install --upgrade pip
	. .venv/bin/activate && pip install -r requirements.txt
	touch .venv/.success

# uncompressed data files
filenames := eHRs-gen2 diabetes-dualaae-ehr-camda
00_origin_data/unzipped/.success: $(addprefix 00_origin_data/unzipped/, $(addsuffix /.success, $(filenames)))
	touch 00_origin_data/unzipped/.success

00_origin_data/unzipped/%/.success: 00_origin_data/zipped/%.zip
	[ -d 00_origin_data/unzipped ] || mkdir -p 00_origin_data/unzipped
	unzip -d 00_origin_data/unzipped/$* $<
	touch 00_origin_data/unzipped/$*/.success

.PRECIOUS: 00_origin_data/zipped/%.zip

# instructions for downloading the data ========================================
download: $(addprefix 00_origin_data/zipped/, $(addsuffix .zip, $(filenames)))

base_url := https://files.chihiro.sbs/fa7b4/
00_origin_data/zipped/%.zip:
	mkdir -p 00_origin_data/zipped
	cd 00_origin_data/zipped && wget $(base_url)$*.zip