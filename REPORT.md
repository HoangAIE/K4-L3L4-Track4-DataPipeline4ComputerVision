# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Ngô Xuân Hoàng |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602597 |
| GitHub | HoangAIE |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-05297 |

## Kết quả cuối

- `dataset_version` của bạn: `24e96a8f3c2e`  (khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: train: 705, val: 163, test: 151 (tổng 1019 khung hình)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (đạt `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và các ảnh bài tập sập/chạy lại (`airflow_task_crash.png`, `airflow_crash_log.png`).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Bốn khác biệt rõ rệt giữa hai nguồn telemetry ở lớp Bronze:
1. Tên cột (schema naming): Nguồn A dùng `ts_utc`, `speed_mps`, `heading_deg`, `lat`, `lon`, `alt_m`, `gas`, `brake`, `steer`; nguồn B dùng `timestamp_ms`, `spd_kmh`, `hdg`, `latitude`, `longitude`, `altitude`, `throttle_pct`, `brake_pct`, `steering`.
2. Đơn vị đo lường (units): Nguồn A tốc độ là m/s, bàn đạp ga/phanh trong khoảng [0, 1]; nguồn B tốc độ là km/h, bàn đạp ga/phanh tính theo phần trăm [0, 100].
3. Định dạng thời gian (time format): Nguồn A dùng chuỗi ISO UTC string (`2025-10-08T14:30:00.000000Z`); nguồn B dùng số nguyên epoch milliseconds (`timestamp_ms`).
4. Độ lệch đồng hồ (clock offset): Nguồn A đồng hồ khớp video (offset 0 ms); nguồn B đồng hồ cảm biến chạy trước video 1500 ms (`telemetry_clock_offset_ms = 1500`).

Pipeline giữ mỗi nguồn một bảng bronze riêng biệt và để mọi cột ở dạng chữ (string) vì theo nguyên lý Medallion (Slide 14), tầng Bronze là nơi lưu trữ dữ liệu thô nguyên bản (raw ingestion, append-only) nhằm đảm bảo tính toàn vẹn 100% không suy đoán kiểu. Do schema giữa các thiết bị bị trôi dạt (schema drift), nếu gộp và ép kiểu ngay thì các dòng chứa ký tự lạ, ô trống hoặc sai định dạng sẽ làm sập quá trình nạp (ingestion failure) và làm mất dữ liệu gốc. Lưu dạng chữ cho phép hấp thụ toàn bộ dữ liệu an toàn, đẩy trách nhiệm làm sạch và phân loại dòng lỗi sang tầng Silver.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Pipeline chuẩn hoá hai nguồn về một schema chung thông qua hai dbt staging model (`stg_telemetry_a.sql` và `stg_telemetry_b.sql`):
- Chuẩn hoá đơn vị: Đổi `spd_kmh / 3.6` thành `speed_mps`; đổi `throttle_pct / 100` và `brake_pct / 100` thành `gas` và `brake` [0, 1]; thống nhất tên cột vị trí và gia tốc.
- Chuẩn hoá thời gian và độ lệch đồng hồ: Nguồn B được trừ độ lệch đồng hồ khai báo từ manifest với công thức `to_timestamp((cast(timestamp_ms as bigint) - offset_ms) / 1000.0) as ts` (với `offset_ms = 1500`).
- Bằng chứng đã đúng: Thời điểm bắt đầu `min(ts)` của telemetry B sau chuẩn hoá là `2025-10-08 14:30:00.009000+00:00`, khớp chính xác với thời điểm bắt đầu của video B `2025-10-08 14:30:00` (độ lệch chỉ 9 ms, thỏa mãn kiểm tra lệch dưới 0.5 s). Tốc độ tối đa hai drive đều hợp lý (dưới 130 km/h: A đạt 79 km/h, B đạt 108 km/h).

Bảng quarantine thực tế từ `silver_telemetry_rejects`:

| drive_id | reject_reason | so_dong |
|---|---|---|
| drive_a | duplicate | 20 |
| drive_a | speed_missing | 30 |
| drive_a | speed_out_of_range | 3 |
| drive_b | duplicate | 20 |
| drive_b | speed_missing | 30 |
| drive_b | speed_out_of_range | 3 |

Tổng cộng mỗi drive có 53 dòng lỗi bị cách ly. Số dòng sạch cộng số dòng cách ly bằng đúng số dòng nhận về (drive A: 2968 + 53 = 3021; drive B: 3181 + 53 = 3234). Dòng lỗi được đưa vào bảng `silver_telemetry_rejects` thay vì xóa bỏ âm thầm để đảm bảo tính minh bạch và khả năng kiểm toán (auditability): kỹ sư cảm biến có thể điều tra nguyên nhân phần cứng hỏng hóc, và nếu sau này có thuật toán nội suy bổ sung, dữ liệu vẫn còn nguyên lineage (`_batch_id`, `_row_no`) để khôi phục.

## Bước 2 · Silver telemetry

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

Test `max_gap_ms` được cấu hình là `warn` vì hiện tượng mất tín hiệu cảm biến tạm thời (sensor dropout do đi qua hầm hoặc che khuất GPS) là khiếm khuyết thường gặp trong môi trường thực tế và có thể chấp nhận được ở một tỷ lệ nhỏ. Nếu để `error`, toàn bộ pipeline sẽ bị ngưng trệ vô cớ. Ngược lại, test `in_range` (tốc độ 0..70 m/s) bắt buộc là `error` vì giá trị tốc độ âm hoặc phi lý (> 252 km/h) là dữ liệu lỗi nghiêm trọng; nếu để lọt vào tập dữ liệu huấn luyện sẽ khiến mô hình điều khiển xe tự lái học sai hành vi vật lý, tiềm ẩn nguy cơ tai nạn chết người.

Kết quả thí nghiệm `break_gate` (từ `report/evidence/break_gate.json`):
Khi nới lỏng luật từ 70 m/s lên 1000 m/s trong dbt, lệnh `dbt test` trả về `PASS=5 WARN=1 ERROR=1 SKIP=0`. Test bắt được lỗi là `in_range_silver_telemetry_speed_mps__70__0`, phát hiện đúng 6 dòng tốc độ phi lý (3 dòng ở drive A và 3 dòng ở drive B). Khi khôi phục lại luật gốc, kết quả kiểm tra quay lại `PASS=6 WARN=1 ERROR=0 SKIP=0`. Kết quả này chứng minh cổng chất lượng (Quality Gate) đóng vai trò lá chắn sống còn: ngăn chặn triệt để dữ liệu xấu lan truyền xuống các tầng downstream theo nguyên tắc "garbage in, garbage out", bảo vệ tính tin cậy của mô hình AI.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Dựa trên file bằng chứng `report/evidence/idempotency.json` (thực hiện trên partition `trip-05297` gồm 31 khung hình):
- Trước khi chạy lại: bảng `silver.frames` có 1246 dòng, dấu vân tay (fingerprint) là `6c7480dfde7e`.
- Sau 2 lần chạy lại liên tiếp với cơ chế Delta overwrite: số dòng vẫn là 1246, dấu vân tay vẫn là `6c7480dfde7e` (hoàn toàn không đổi, `unchanged: true`).
- Khi chạy thử nghiệm kiểu ghi `append` sai 2 lần: số dòng của trip tăng gấp đôi từ 31 lên 62 dòng (nhân đôi 2x).

Cơ chế Delta `overwrite` theo partition (dùng predicate `trip_id = 'trip-xxxxx'`) an toàn vượt trội so với lệnh xóa (`DELETE`) rồi ghi lại (`INSERT`) nhờ tính chất giao dịch ACID nguyên tử:
1. Tính nguyên tử (atomic commit): Delta Lake ghi các file Parquet mới vào storage trước, sau đó ghi một commit mới vào `_delta_log`. Người đọc (readers) luôn nhìn thấy một snapshot nhất quán, không bao giờ bị thấy bảng trống (zero-row gap) như khi thực hiện DELETE rồi INSERT.
2. An toàn khi gặp sự cố giữa chừng: Nếu quá trình trích xuất bị ngắt đột ngột (crash, mất điện, tràn đĩa), commit chưa được tạo nên Delta Lake bỏ qua các file rác dở dang; bảng vẫn giữ nguyên vẹn dữ liệu của phiên bản trước đó mà không bị mất trắng dữ liệu của trip.

## Bước 3 · Silver frames

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

Khung hình đen (`is_black`) là lỗi phần cứng hoặc kỹ thuật (nắp che ống kính đóng, đứt cáp camera, mất luồng hình ảnh) khiến điểm ảnh hoàn toàn không có thông tin thị giác; pipeline phát hiện và loại bỏ triệt để không đưa vào Gold. Ngược lại, khung hình tối (`is_dark`) xuất hiện tự nhiên khi xe đi qua đường hầm, hẻm núi hoặc bóng râm; hình ảnh tuy thiếu sáng nhưng vẫn chứa các đặc trưng quang học (vạch kẻ đường, đèn xe, chướng ngại vật). Pipeline gắn cờ `is_dark` nhưng giữ lại (`dark_kept`: drive B có tới 61 khung tối được giữ lại). Tôi hoàn toàn đồng ý với thiết kế này vì mô hình xe tự lái cần dữ liệu thiếu sáng để rèn luyện độ bền vững (robustness), chỉ loại bỏ khi camera thực sự hỏng.

Bảng số khung trùng theo ngưỡng dHash từ `report/evidence/threshold.json`:

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Tôi chọn ngưỡng **2** (ngưỡng pipeline đang áp dụng). Lý do: ngưỡng 0 quá khắt khe, chỉ bắt khung giống hệt từng bit (bit-identical), bỏ lọt các khung thực chất bị đứng hình nhưng điểm ảnh bị nhiễu cảm biến nhẹ; ngưỡng 6 quá lỏng lẻo, loại bỏ tới 363 khung (A) và 244 khung (B), làm mất cả những khung hình xe đang di chuyển thực sự. Ngưỡng 2 loại bỏ chính xác 11/12 khung camera bị đứng hình 6 giây ở drive A mà không làm mất thông tin hữu ích. Ngưỡng này phụ thuộc chặt chẽ vào tốc độ xe: khi xe chạy nhanh, cảnh vật biến đổi liên tục giữa 2 khung hình (khoảng cách Hamming dHash lớn); khi xe dừng chờ đèn đỏ hoặc bò chậm, cảnh vật gần như bất biến khiến dHash rất gần nhau. Do đó ở tốc độ thấp cần ngưỡng nhỏ để tránh nhận nhầm xe di chuyển chậm là đứng hình.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Số liệu từ `report/evidence/leakage.json` (tổng cộng 1019 khung hình hợp lệ thuộc 26 trip):
- Chia THEO KHUNG HÌNH (ngẫu nhiên 70/15/15): Có tới **26/26 trip** bị chia cắt nằm ở nhiều hơn một split, và cả **26/26 trip** đều có khung hình xuất hiện đồng thời ở cả tập train lẫn tập test (`per_frame_split_trips_in_both_train_and_test = 26`).
- Chia THEO TRIP (Gold của pipeline): Có đúng **0 trip** bị chia cắt (`per_trip_split_trips_straddling = 0`).

Điều này khiến điểm kiểm thử đẹp một cách giả tạo vì hiện tượng rò rỉ dữ liệu (data leakage) do tương quan thời gian cao (temporal correlation): các khung hình quay liên tiếp ở 10 fps (hoặc lấy mẫu 2 fps) chỉ cách nhau nửa giây nên ngoại cảnh, góc chiếu, điều kiện thời tiết và mặt đường gần như giống hệt nhau. Khi chia ngẫu nhiên theo khung hình, mô hình đã "nhìn thấy" khung hình $t$ trong tập huấn luyện và được chấm điểm trên khung hình $t+1$ ở tập kiểm thử. Mô hình chỉ việc ghi nhớ (overfitting/memorization) bối cảnh cụ thể thay vì học được khả năng khái quát hóa (generalization) cho cảnh mới. Kết quả đánh giá trên tập test sẽ rất cao giả tạo, nhưng khi triển khai ra thực tế trên cung đường mới xe sẽ lập tức phán đoán sai.

## Bước 4 · Gold

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` được thiết kế là mã băm nội dung (content-addressable hash) thay vì ngày giờ hay số đếm để đảm bảo tính tất định và khả năng tái lập (reproducibility): cùng dữ liệu đầu vào và cùng quy tắc xử lý sẽ luôn sinh ra cùng một mã hash duy nhất, độc lập với thời điểm hay máy thực thi. Nếu dùng ngày giờ hoặc số tăng dần, mỗi lần retry hoặc chạy lại pipeline sẽ vô cớ sinh ra một phiên bản mới dù nội dung không đổi bit nào, gây lãng phí tài nguyên và sai lệch ML metadata tracking. Hai lần chạy liên tiếp trên cùng dữ liệu cho ra cùng một `dataset_version` duy nhất (`24e96a8f3c2e`), đảm bảo tính idempotent.

`dataset_version` của tôi khác bạn bè dù dùng chung tập video thô vì trong `gold_dataset_frames.sql`, biến môi trường `LAB_STUDENT_ID` (MSSV = `2A202602597`) được dùng làm giá trị muối (salt) để sắp xếp và phân chia split:
`order by md5(trip_id || '{{ env_var("LAB_STUDENT_ID", "") }}')`
Do MSSV của mỗi học viên là duy nhất, thứ tự băm của 26 trip thay đổi, dẫn đến danh sách trip được gán vào train, val, test khác nhau, số khung hình mỗi split khác nhau, từ đó mã băm nội dung của manifest cuối cùng hoàn toàn khác nhau giữa các sinh viên.

## Bước 4 · Gold

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Trích xuất trực tiếp từ phần `lineage` trong `manifest.json` của phiên bản `24e96a8f3c2e`:
Dataset này được tạo từ hai video gốc có mã SHA-256 xác thực:
- `drive_a`: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`
- `drive_b`: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`
Cùng các snapshot phiên bản chính xác của các bảng Delta thượng nguồn:
- `bronze.raw_telemetry_a`: commit version 0
- `bronze.raw_telemetry_b`: commit version 0
- `bronze.trips_raw`: commit version 0
- `bronze.video_files`: commit version 1
- `silver.frames`: commit version 25
- `silver.telemetry`: commit version 0
- `silver.frames_flagged`: commit version 0
- `silver.frame_telemetry`: commit version 0

Nếu 3 tháng sau cần tái tạo chính xác dataset này, những thông tin cốt lõi giúp tái tạo là:
1. Hai mã SHA-256 của file video gốc để đảm bảo đúng nguồn dữ liệu không bị sửa đổi.
2. Số phiên bản commit của các bảng Delta để sử dụng tính năng Time Travel (`VERSION AS OF`) khôi phục đúng trạng thái bảng thời điểm đó.
3. Giá trị salt `LAB_STUDENT_ID=2A202602597` và bộ quy tắc lọc ghi trong `rules` (`drop: [is_black, is_blurry, is_near_dup, not sync_ok]`, phân chia theo trip).

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Diễn biến thực tế khi thực hiện bài tập sập và chạy lại với trip cá nhân `trip-05297` (map_index 15):
1. Khi đặt `LAB_FAIL_ONCE=trip-05297` và trigger DAG, đúng **1 task chuyển đỏ (failed)**: đó là task instance `silver_frames` với `map_index = 15`. Log ghi nhận biệt lệ: `RuntimeError: simulated crash while extracting trip-05297; run it again`.
2. Đúng **7 task phía sau chuyển `upstream_failed`**: `dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`.
3. Tôi tiến hành Clear task đỏ `silver_frames` map_index 15 kèm downstream: task chạy lại thành công (do file marker `.failed-trip-05297` đã tồn tại), kéo theo toàn bộ 7 task downstream thực thi thành công và cả DAG chuyển xanh hoàn toàn.
4. Dữ liệu **hoàn toàn không bị nhân đôi** nhờ cơ chế partition overwrite của Delta Lake.
5. `dataset_version` **hoàn toàn không đổi**, vẫn giữ nguyên giá trị `24e96a8f3c2e`.
6. Bằng chứng: Ảnh chụp log lỗi task đỏ `report/screenshots/airflow_crash_log.png` (thấy rõ `trip-05297`), ảnh đồ thị khi sập `airflow_task_crash.png`, ảnh DAG xanh hoàn chỉnh `airflow_graph.png`, và kết quả tự động kiểm tra `report/verify.txt` đạt 27/27 checks passed.

## Bước 5 · Điều phối và phục vụ

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Khi quy mô tăng từ 2 drive lên 500 xe mỗi ngày (tương đương ~6.500 trip, hàng triệu khung hình và hàng chục TB dữ liệu mỗi ngày), ba thành phần sẽ vỡ đầu tiên:
1. Trích xuất video tuần tự bằng FFmpeg trên container Airflow đơn lẻ (Slide 23 - Nút cổ chai giải mã video): Hiện tại FFmpeg chạy trên CPU của node Airflow. Với 500 xe, giải mã hàng chục TB video sẽ làm CPU và I/O quá tải, mất hàng trăm giờ. Thay thế bằng: Kiến trúc giải mã phân tán (Distributed Video Decoding) dùng cụm worker Ray hoặc Spark on Kubernetes, tận dụng GPU (NVDEC) giải mã song song hàng loạt video chunk.
2. Engine biến đổi DuckDB in-process chạy trên file cục bộ (Slide 22 - In-process vs Distributed Engine): DuckDB chạy trong bộ nhớ của worker container và bị giới hạn bởi RAM/ổ đĩa cục bộ (`lakehouse.duckdb`). Hàng chục triệu dòng telemetry sẽ gây lỗi cạn kiệt bộ nhớ (OOM) và tranh chấp khóa file khi nhiều tiến trình cùng truy cập. Thay thế bằng: Công cụ tính toán phân tán như Apache Spark hoặc Trino kết hợp với Open Table Format (Delta Lake / Apache Iceberg) lưu trữ trên object storage quy mô lớn (S3/Ceph).
3. Kho dữ liệu đơn node Postgres phục vụ dashboard (Slide 12, 16 - Warehouse phục vụ BI): Bảng Postgres đơn node sẽ bị nghẽn I/O khi lưu trữ hàng chục triệu bản ghi gallery và marts, khiến truy vấn tổng hợp trên Metabase bị treo. Thay thế bằng: Hệ quản trị cơ sở dữ liệu phân tích hướng cột (Distributed Columnar OLAP) chuyên dụng như ClickHouse, StarRocks hoặc Cloud Warehouse (Snowflake / BigQuery).

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

```sql
SELECT 
    trip_id,
    drive_id,
    lighting,
    conditions,
    split,
    frames AS tong_khung,
    frames_kept AS khung_giu_lai,
    (frames - frames_kept) AS khung_bi_loai,
    ROUND((frames - frames_kept)::numeric / frames * 100, 1) AS ty_le_loai_pct,
    avg_speed_kmh
FROM marts.mart_trip_summary
ORDER BY ty_le_loai_pct DESC;
```

Ý nghĩa và phát hiện từ câu truy vấn:
Câu truy vấn phân tích chi tiết tỷ lệ đào thải khung hình (frame drop rate) của từng chuyến đi cùng các yếu tố vận hành (ánh sáng, thời tiết, tốc độ di chuyển). Kết quả chỉ ra rằng `trip-04622` (drive A) có tỷ lệ loại bỏ khung hình cao nhất lên tới **56.7%** (34/60 khung bị loại) và `trip-03904` đạt **50.0%**. Nguyên nhân chủ yếu là do camera gặp sự cố đứng hình 6 giây liên tục kết hợp với xe dừng/di chuyển chậm tạo ra lượng lớn khung hình trùng lặp (`is_near_dup`). Ngược lại, các trip có tốc độ cao như `trip-04324` (81.9 km/h) giữ lại trọn vẹn 100% khung hình (0% bị loại). Điều này giúp đội ngũ vận hành xác định chính xác những chuyến đi nào bị suy giảm chất lượng dữ liệu nhiều nhất để ưu tiên bảo trì cảm biến và tối ưu hóa chu kỳ lấy mẫu.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Điều bất ngờ nhất: Cơ chế phân vùng (partition) kết hợp với Delta Lake transactional overwrite và việc trộn MSSV vào hàm băm giúp biến một pipeline phức tạp thành hệ thống mang tính tất định tuyệt đối: mỗi sinh viên có một dataset độc bản nhưng vẫn hoàn toàn kiểm chứng được tính đúng đắn.
Điều còn băn khoăn: Việc đồng bộ thời gian (ASOF join) giữa video và telemetry hiện dùng sai số đồng hồ cố định toàn chuyến (1.5 s ở drive B); nếu thiết bị gặp hiện tượng trôi dạt đồng hồ phi tuyến (clock drift tích lũy theo thời gian) thì phương pháp cộng trừ hằng số tuyến tính sẽ dần mất độ chính xác.
Lỗi dữ liệu pipeline chưa bắt được: Pipeline hiện chưa phát hiện được hiện tượng camera bị bẩn ống kính một phần (ví dụ vết bùn, hạt mưa đọng che khuất biển báo) hoặc hiện tượng chói lóa ngược sáng cực độ (sun flare) khiến vùng ảnh quan trọng bị bão hòa trắng xóa dù giá trị độ sáng trung bình toàn khung vẫn ở mức hợp lệ.
