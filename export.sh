#!/bin/sh

# 位置参照情報ダウンロードサービス (http://nlftp.mlit.go.jp/isj/) からデータを取得して SQLite データベースに出力する。

set -e

DEST="${1:-isj.sqlite3}" # 出力

# データの「版数」。以下のコマンドの出力として得られる
# curl -G 'http://nlftp.mlit.go.jp/isj/api/1.0b/index.php/app/getISJURL.xml' --data-urlencode 'appId=isjapibeta1' --data-urlencode "fiscalyear='平成30年'" --data-urlencode 'posLevel=0,1' --data-urlencode 'areaCode=01000' | xmllint --xpath '/ISJ_URL_INF/ISJ_URL/item/verNumber' -
GAIKU='17.0a' # 街区レベル位置参照情報
OOAZA='12.0b' # 大字・町丁目レベル位置参照情報

YEAR='2018' # データの年度

gem list -i "^sqlite3$" > /dev/null || (echo 'This script requires `sqlite3` gem' >&2 && exit 1)

# カレントディレクトリに作業用ディレクトリを作る
mkdir -p downloads # ZIP ファイルのダウンロード先
mkdir -p "data/$GAIKU" "data/$OOAZA" # CSV ファイルの展開先

# データの取得
if [ ! -e downloads/complete ]; then
  seq -w 1 47 \
    | parallel --arg-file - "
        curl -C - \
          -o 'downloads/{1}000-{2}.zip' \
          'http://nlftp.mlit.go.jp/isj/dls/data/{2}/{1}000-{2}.zip'
        unzip -p \
          'downloads/{1}000-{2}.zip' \
          '{1}000-{2}/{1}_$YEAR.csv' \
          | tail -n +2 \
          | gzip -9 \
          > 'data/{2}/{1}_$YEAR.csv.gz'
      " ::: "$GAIKU" "$OOAZA"

  touch downloads/complete
fi

# データベースへの書き出し

# スキーマ
sqlite3 "$DEST" <<SQL
BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS prefectures ( -- 都道府県
  id INTEGER NOT NULL PRIMARY KEY, -- ID。都道府県コードと同一
  name TEXT NOT NULL UNIQUE -- 都道府県名
);

CREATE TABLE IF NOT EXISTS cities ( -- 市区町村
  id INTEGER NOT NULL PRIMARY KEY,
  code INTEGER, -- 市区町村コード。残念ながら元データがユニークでないため UNIQUE としない
  prefecture INTEGER NOT NULL REFERENCES prefectures(id) ON DELETE RESTRICT,
  name TEXT NOT NULL -- 市区町村名
);

CREATE TABLE IF NOT EXISTS ooazas ( -- 大字・町・丁目
  id INTEGER NOT NULL PRIMARY KEY,
  code INTEGER, -- 大字町丁目コード
  city INTEGER NOT NULL REFERENCES cities(id) ON DELETE RESTRICT,
  name TEXT NOT NULL, -- 大字町丁目名
  latitude REAL, -- 緯度
  longitude REAL -- 経度
);

CREATE TABLE IF NOT EXISTS koazas ( -- 小字・通称名
  id INTEGER NOT NULL PRIMARY KEY,
  ooaza INTEGER NOT NULL REFERENCES ooazas(id) ON DELETE RESTRICT,
  name TEXT NOT NULL -- 名前。小字が存在しない場合は空白とする
);

CREATE TABLE IF NOT EXISTS gaikus ( -- 街区
  number INTEGER NOT NULL, -- 街区符号・地番。ユニークでなく、また数字であると限らないため主キーとしない
  koaza INTEGER NOT NULL REFERENCES koazas(id) ON DELETE RESTRICT,
  latitude REAL NOT NULL, -- 緯度
  longitude REAL NOT NULL, -- 経度
  representative INTEGER NOT NULL -- 代表フラグ
);
CREATE INDEX IF NOT EXISTS idx_gaikus_koaza ON gaikus(koaza);

-- 以下は書き出しの進捗状況の記録用

CREATE TABLE IF NOT EXISTS ooazas_imported ( -- 大字・町丁目レベル位置参照情報の書き出しが完了している都道府県
  id INTEGER NOT NULL PRIMARY KEY REFERENCES prefectures(id)
);

CREATE TABLE IF NOT EXISTS gaikus_imported ( -- 街区レベル位置参照情報の書き出しが完了している都道府県
  id INTEGER NOT NULL PRIMARY KEY REFERENCES prefectures(id)
);

COMMIT;
SQL

ruby <<RUBY
  # frozen_string_literal: true

  require 'csv'
  require 'sqlite3'
  require 'zlib'

  STDERR.set_encoding(Encoding::UTF_8)

  SQLite3::Database.new('$DEST') do |db|
    db.execute('PRAGMA foreign_keys=ON')

    # 大字・町丁目レベル位置参照情報

    is_imported = db.prepare(<<-SQL)
      SELECT ? IN ooazas_imported;
    SQL
    mark_as_imported = db.prepare(<<-SQL)
      INSERT INTO ooazas_imported (id) VALUES (?);
    SQL
    insert_pref = db.prepare(<<-SQL)
      INSERT
        INTO prefectures (id, name)
        VALUES (?, ?);
    SQL
    insert_city = db.prepare(<<-SQL)
      INSERT
        INTO cities (code, prefecture, name)
        VALUES (?, ?, ?);
    SQL
    insert_ooaza = db.prepare(<<-SQL)
      INSERT
        INTO ooazas (code, city, name, latitude, longitude)
        VALUES (?, ?, ?, ?, ?);
    SQL

    # CSV の converters 用
    ITSELF = Proc.new {|x| x }
    TO_I = Proc.new {|x| x.to_i }

    print 'Exporting 大字・町丁目レベル位置参照情報... '

    begin
      (1..47).each do |pref_id|
        next if is_imported.execute(pref_id).first.first == 1

        if STDOUT.isatty
          print "\033[2K\r"
          print "Exporting 大字・町丁目レベル位置参照情報... (#{pref_id}/47)"
        end

        path = "data/$OOAZA/#{pref_id.to_s.rjust(2, '0')}_$YEAR.csv.gz"
        # エンコーディングは Shift_JIS と記載されているがこれは不正確で、実際は CP932 が必要。
        csv = Zlib::GzipReader.open(path, external_encoding: Encoding::Windows_31J)
        rows = CSV.new(csv, converters: Proc.new do |f, info|
          case info.index; when 0, 2, 4; f.to_i; else; f; end
        end).
          # 市区町村 ID を再利用するために同一の地域が連続で並ぶようにソート（元データは完全にはソートされていない）
          sort_by {|_, _, city_code| city_code }

        db.transaction

        pref = rows[0][1]
        insert_pref.execute(pref_id, pref)

        rows.chunk {|(_, _, city_code, city)| [city_code, city] }.each do |(city_code, city), rows|
          insert_city.execute(city_code, pref_id, city)
          city_id = db.last_insert_row_id
          rows.each do |(_, _, _, _, ooaza_code, ooaza, lat, lon, _ref, _kind)|
            insert_ooaza.execute(ooaza_code, city_id, ooaza, lat, lon)
          end
        end

        mark_as_imported.execute(pref_id)

        db.commit
      end

      if STDOUT.isatty
        print "\033[2K\r"
        puts 'Exporting 大字・町丁目レベル位置参照情報... Done'
      else
        puts 'Done'
      end
    ensure
      [is_imported, mark_as_imported, insert_pref, insert_city, insert_ooaza].each(&:close)
    end

    # 街区レベル位置参照情報

    is_imported = db.prepare(<<-SQL)
      SELECT ? IN gaikus_imported;
    SQL
    mark_as_imported = db.prepare(<<-SQL)
      INSERT INTO gaikus_imported (id) VALUES (?);
    SQL
    get_city = db.prepare(<<-SQL)
      SELECT id FROM cities WHERE prefecture = ? AND name = ?;
    SQL
    insert_city = db.prepare(<<-SQL)
      INSERT INTO cities (prefecture, name) VALUES (?, ?);
    SQL
    get_ooaza = db.prepare(<<-SQL)
      SELECT id FROM ooazas WHERE city = ? AND name = ?;
    SQL
    insert_ooaza = db.prepare(<<-SQL)
      INSERT INTO ooazas (city, name) VALUES (?, ?);
    SQL
    insert_koaza = db.prepare(<<-SQL)
      INSERT INTO koazas (ooaza, name) VALUES (?, ?);
    SQL
    insert_gaiku = db.prepare(<<-SQL)
      INSERT
        INTO gaikus (number, koaza, latitude, longitude, representative)
        VALUES (?, ?, ?, ?, ?);
    SQL

    begin
      (1..47).each do |pref_id|
        next if is_imported.execute(pref_id).first.first == 1

        path = "data/$GAIKU/#{pref_id.to_s.rjust(2, '0')}_$YEAR.csv.gz"

        print "Loading #{path}... "

        csv = Zlib::GzipReader.open(path, external_encoding: Encoding::Windows_31J)
        # ソートのために全体を読み込む。1 件あたり高々 300 MB（展開後）なのでコストは許容範囲内とみなす
        rows = CSV.new(csv).reject do |(pref)|
          # 何故か住所に空文字列が指定されている行が何件か存在するので、それらをスキップ
          pref.empty?
        end
        rows.sort_by! {|row| row[1..3] }

        puts 'Done'

        print 'Exporting to the database... '

        # 進捗表示
        progress = 0 # 書き出した行数
        done = false
        if STDOUT.isatty
          rows_count = rows.length
          Thread.new do
            loop do
              percentage = (100 * progress / rows_count.to_f).round(1).to_s.rjust(5)
              progress_s = progress.to_s.rjust(rows_count.to_s.length)
              break if done
              print "\033[2K\rExporting to the database... (#{progress_s}/#{rows_count}) (#{percentage} %)"
              sleep 1
            end
          end
        end

        db.transaction

        pref = rows[0][0] # 都道府県名

        rows.chunk {|(_, city)| city }.each do |city, rows|
          result = get_city.execute(pref_id, city).to_a
          if result.length > 1
            # 稀に同一の市区町村名に複数の市区町村コードが割り当てられている
            if STDOUT.isatty
              print "\033[2K\r"
            else
              puts
            end
            STDERR.puts("warning: duplicate city codes for: #{pref} #{city}. Using the first one found")
          end
          ((city_id,)) = result
          unless city_id
            # 稀に大字・町丁目レベル位置参照情報に存在しない市区町村名が混ざっている
            if STDOUT.isatty
              print "\033[2K\r"
            else
              puts
            end
            STDERR.puts "warning: missing city code for: #{pref} #{city}"
            insert_city.execute(pref_id, city)
            city_id = db.last_insert_row_id
          end

          rows.chunk {|(_, _, ooaza)| ooaza }.each do |ooaza, rows|
            result = get_ooaza.execute(city_id, ooaza).to_a
            if result.length > 1
              if STDOUT.isatty
                print "\033[2K\r"
              else
                puts
              end
              STDERR.puts("warning: duplicate ooaza codes for: #{pref} #{city} #{ooaza}. Using the first one found" )
            end
            ((ooaza_id,)) = result
            unless ooaza_id
              if STDOUT.isatty
                print "\033[2K\r"
              else
                puts
              end
              STDERR.puts("warning: missing ooaza code for: #{pref} #{city} #{ooaza}")
              insert_ooaza.execute(city_id, ooaza)
              ooaza_id = db.last_insert_row_id
            end

            rows.chunk {|(_, _, _, koaza)| koaza }.each do |koaza, rows|
              insert_koaza.execute(ooaza_id, koaza)
              koaza_id = db.last_insert_row_id

              rows.each do |(pref, _, _, _, gaiku, _, _, _, lat, lon, _, repr, _, _)|
                progress += 1
                insert_gaiku.execute(gaiku, koaza_id, lat, lon, repr)
              end
            end
          end
        end

        mark_as_imported.execute(pref_id)

        db.commit

        if STDOUT.isatty
          done = true
          print "\033[2K\r"
          puts "Exporting to the database... Done"
        else
          puts 'Done'
        end
      end
    ensure
      [is_imported, mark_as_imported, get_city, insert_city, get_ooaza, insert_ooaza, insert_koaza, insert_gaiku].each(&:close)
    end

    # 全ての大字の下に便宜的に無名の小字を割り当てる

    missing_koazas = db.prepare(<<-SQL)
      SELECT id FROM ooazas WHERE id NOT IN (SELECT ooaza FROM koazas WHERE name = '');
    SQL

    db.transaction

    begin
      missing_koazas.execute.each do |(id)|
        db.execute(<<-SQL, id)
          INSERT INTO koazas (ooaza, name) VALUES (?, '');
        SQL
      end
      db.commit
    ensure
      missing_koazas.close
    end
  end
RUBY
