#!/usr/bin/env ruby

# frozen_string_literal: true

# 住所から位置参照情報を得る簡易検索ツール。
# 標準入力から住所らしき文字列を受け取り、位置参照情報データベースから座標情報を取り出して標準出力にタブ区切りで出力する。
# 出力の各列の内容:
# - 1: 与えられた住所文字列
# - 2: データベースでマッチした住所文字列
# - 3: 緯度
# - 4: 経度
# - 5: データの種別（0: 街区レベル位置参照情報、1: 大字・町丁目レベル位置参照情報）

require 'csv'
require 'sqlite3'


# [1, 10000) の整数を漢数字にする
def itok(n)
  raise Math::DomainError unless (1..9999).include?(n)

  exp = 0
  d = 1
  m = n

  until m < 10
    d *= 10
    exp += 1
    m /= 10
  end

  # いま d は n 以下で最大の 10 の冪であって、exp = log10(d)

  ret = +''

  # d と exp を減らしながら n の最上位の桁から順番に処理
  until exp == 0
    digit = n / d % 10 # 現在の桁の値
    ret.concat(['', '', '二', '三', '四', '五', '六', '七', '八', '九'][digit])
    unless digit == 0
      ret.concat([nil, '十', '百', '千'][exp])
    end
    d /= 10
    exp -= 1
  end

  # 一の位
  ret.concat(['', '一', '二', '三', '四', '五', '六', '七', '八', '九'][n / d % 10])

  ret
end

# 住所にありがちな表記揺れを吸収する
def normalize_address(s)
  s.
    tr('ヶ', 'ケ'). # 「〇〇ヶ丘」 <=> 「〇〇ケ丘」
    gsub(/大?字/, '') # 「××町字〇〇」 <=> 「××町〇〇」
end

out = CSV.new(STDOUT, col_sep: "\t")

SQLite3::Database.new(ARGV[0] || 'isj.sqlite3') do |db|
  search_koaza = db.prepare(<<-SQL)
    SELECT koazas.id, prefectures.name, cities.name, ooazas.name, koazas.name, latitude, longitude
      FROM koazas
        JOIN ooazas ON ooazas.id = ooaza
        JOIN cities ON cities.id = city
        JOIN prefectures ON prefectures.id = prefecture
      WHERE
        ooazas.city IN (SELECT id FROM cities WHERE replace(?1, '%', '') LIKE '%'||name||'%')
        AND (
          prefectures.name||cities.name||ooazas.name||koazas.name LIKE ?1
          OR replace(?1, '%', '')
            LIKE prefectures.name||cities.name
              || replace(replace(ooazas.name, '大字', '')||koazas.name, '字', '')||'%'
        )
  SQL
  search_gaiku = db.prepare(<<-SQL)
    SELECT latitude, longitude, representative
      FROM gaikus
      WHERE koaza = ? AND number = ?
  SQL

  begin
    STDIN.each_line do |addr|
      addr.chomp!

      a = addr.tr('０１２３４５６７８９', '0123456789') # 全角数字を半角に
      a.gsub!(/\s+/, '') # 空白文字を取り除く

      # 最初に算用数字が現れる部分（「1丁目」など）を境に分割
      (name, nums) = a.match(/^([^\d]*)(.*)$/)[1..]

      # 数字から始まる文字列に分割（"1丁目2-3" => ["1丁目", "2-", "3"]）
      nums = nums.split(/(?<=[^\d])(?=\d)/)

      # データベース上で丁目名が漢数字で表記されているのに合わせる
      if nums.first&.match(/^(\d)+丁目\s*/)
        nums.shift
        name.concat(itok(Integer($1)) + '丁目')
      end

      # 小字の特定

      results = search_koaza.execute(normalize_address(name).gsub('', '%')).to_a
      if results.length == 1
        result = results[0]
      elsif results.length > 1
        # 複数の候補から絞り込みを試みる

        unless name.end_with?('丁目') || nums[0].include?('番')
          # 数字部分と対応する丁目名が存在すればそれを採用
          chome_candidate = itok(nums[0]&.to_i) + '丁目'
          result = results.find {|(_, _, _, ooaza)| ooaza.end_with?(chome_candidate) }
          nums.shift if result
        end

        unless result
          # 完全一致するものが存在すればそれを採用
          result = results.find do |(_, pref, city, ooaza, koaza)|
            normalized_name = normalize_address(name)
            normalize_address([pref, city, ooaza, koaza].join) == normalized_name ||
              normalize_address([city, ooaza, koaza].join) == normalized_name
          end
        end

        # 丁目名なしの大字がただ 1 つだけ存在すればそれを採用
        unless result
          results.reject! {|(_, _, _, ooaza)| ooaza.match(/(:?\d+|[一二三四五六七八九十百]+)丁目$/) }
          if results.length == 1
            result =  results[0]
          else
            # 今回はこれで一意に決定できると仮定
            raise("Could not determine a koaza in: #{results}") unless result
          end
        end
      else # results.length == 0
        # お手上げ
        out.add_row([addr, nil, nil, nil, nil])
        next
      end

      raise 'expected a result' unless result

      (koaza_id, pref, city, ooaza, koaza, ooaza_lat, ooaza_lon) = result

      # 街区の特定

      if nums[0] # 街区番号
        number = Integer(nums[0].match(/^\d+/)[0])
        results = search_gaiku.execute(koaza_id, number).to_a
        # 代表フラグが設定されたものを優先するが、なければとりあえず先頭のものを取る
        result = results.find {|(_, _, repr)| repr } || results.first
      else
        result = nil
      end

      if result
        (lat, lon) = result
        out.add_row([addr, [pref, city, ooaza, koaza, number].join, lat, lon, 0])
      else # 街区レベルの情報がなければ大字レベルのもので代替する
        out.add_row([addr, [pref, city, ooaza, koaza].join, ooaza_lat, ooaza_lon, 1])
      end
    end
  ensure
    [search_koaza, search_gaiku].each(&:close)
  end
end
