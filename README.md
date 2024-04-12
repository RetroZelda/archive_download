# archive_download
Script to automate downloading from various archival sites like archive.org or myrient where a collection of links exist in an html table

usage:
    python3 ./archive_download.py -u {url}

    arguments:
        -u: [required] url to download from
        -o: output directory for file (defaults to ./output)
        -d: database directory to resume state or scan for updates (defaults to ./.db)
        -t: number of threads to use (defaults to 1)
        -s: number of items to skip (defaults to 1 usually to skip header lines)

