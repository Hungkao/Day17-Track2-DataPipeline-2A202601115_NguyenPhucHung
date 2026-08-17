# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Phúc Hưng  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 46.4s
  run 2/3 … 42.0s
  run 3/3 … 42.7s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau mỗi lần chạy lại pipeline hoặc khi bấm Clear Task trên Airflow, số lượng hàng của `gold_training_set` tăng đột biến (từ 12,480 lên 38,750 hàng), xuất hiện 12,480 ticket bị trùng lặp. |
| **Nguyên nhân** | Model `gold_training_set.sql` cấu hình incremental nhưng thiếu `unique_key` và `incremental_strategy`, khiến dbt mặc định sinh câu lệnh `INSERT` thuần (append) thay vì `MERGE`. Vì nguồn CDC có bản ghi cập nhật `op='u'` rơi vào nhiều partition ngày khác nhau, các bản ghi này bị ghi thêm nhiều lần. Đồng thời trong Airflow DAG, `catchup=True` và `max_active_runs=None` khiến thao tác Clear Task tự động kích hoạt backfill đồng thời, gây race condition và nhân bản dữ liệu. |
| **Cách khắc phục** | • `dbt/models/gold/gold_training_set.sql`: Bổ sung `unique_key='ticket_id'` và `incremental_strategy='merge'` trong khối `config()`.<br>• `dags/ai_training_pipeline.py`: Đặt `catchup=False` và `max_active_runs=1`. |
| **Bằng chứng** | trước: 38,750 hàng (12,480 ticket bị lặp) · sau: 12,480 hàng (1 hàng / 1 ticket) · checksum 3 lượt: `8dd7c98653` (100% trùng khớp, ỔN ĐỊNH ✓). |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_feature_daily` bị thiếu 455 hàng so với kỳ vọng (đạt 8,645 / 9,100 hàng), các hàng bị thiếu tập trung ở các ngày trong quá khứ. |
| **P99 độ trễ đo được** | **2.73 ngày** *(chính xác: 2.7258 ngày ~ 65.4 giờ; tỷ lệ late > 1 ngày: 5.05%)* |
| **Lookback đã chọn** | **3 ngày** — vì P99 = 2.73 ngày, làm tròn lên cửa sổ 3 ngày sẽ bao trùm > 99% các bản ghi đến muộn (late-arriving data). |
| **Nguyên nhân** | Điều kiện lọc incremental ban đầu là `where event_date > (select max(event_date) from {{ this }})`. Khi một sự kiện xảy ra ngày D nhưng đến kho muộn ở ngày D+k (khi kho đã có `max(event_date) >= D`), điều kiện so sánh lớn hơn nghiêm ngặt `>` sẽ bỏ qua toàn bộ các sự kiện quá khứ này. |
| **Cách khắc phục** | • `dbt/models/gold/gold_feature_daily.sql`: Sửa điều kiện lọc thành `where event_date >= (select max(event_date) - interval '3 day' from {{ this }})`.<br>• Bổ sung `unique_key=['event_date', 'customer_id']` và `incremental_strategy='delete+insert'` để ghi đè idempotently khi tính toán lại cửa sổ lookback. |
| **Bằng chứng** | trước: 8,645 hàng · sau: 9,100 hàng (đủ 100% 14 ngày × 650 customer) · checksum 3 lượt: `3db448685c` (ỔN ĐỊNH ✓). |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> `max` phụ thuộc vào các trường hợp ngoại lai cực đoan (outliers do sự cố mạng kéo dài hoặc phục hồi backup), có thể lên tới hàng tuần hoặc hàng tháng. Nếu chọn `max`, mỗi batch chạy hàng ngày bắt buộc phải quét và tính toán lại toàn bộ lịch sử partition dài ngày, gây lãng phí chi phí tính toán (compute cost), I/O và tăng runtime tích lũy ở mọi lượt chạy sau này. Chọn P99 là điểm cân bằng tối ưu giữa việc bao phủ hầu hết dữ liệu trễ thông thường với chi phí tính toán tối thiểu (chỉ quét lại 3 ngày). Các bản ghi ngoại lai trễ vượt P99 (< 1%) sẽ được xử lý bằng batch reconciliation định kỳ thay vì gánh nặng trên pipeline hàng ngày.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Cột `priority` bị đổi từ số sang chuỗi ở nguồn, khiến `silver_tickets` xuất hiện 6,606 hàng sai / NULL, làm giảm chất lượng mô hình phân loại. `quarantine_tickets` không hứng được bản ghi lỗi nào. |
| **Nguyên nhân** | `try_cast(priority_raw as integer)` làm mất toàn bộ dữ liệu khi backend đổi sang nhãn chuỗi ('urgent', 'high', ...), đồng thời lại chấp nhận các số không hợp lệ ('0', '5', '-1'). Thứ tự trong `silver_tickets` xếp hạng `row_number()` trước khi lọc khiến ticket bị mất nếu bản ghi mới nhất bị lỗi. Data contract chưa được kích hoạt (`enforced: false`). |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | 1. **Số hợp lệ** (`1`, `2`, `3`, `4`): Giữ nguyên kiểu integer.<br>2. **Nhãn chuỗi** (`urgent`, `high`, `medium`, `low`): Map sang số tương ứng (`urgent→1`, `high→2`, `medium→3`, `low→4`).<br>3. **Dữ liệu lỗi** (`unknown`, `P1`, `P2`, `0`, `5`, `-1`, `''`, `NULL`): Trả về `NULL` và chuyển sang `quarantine_tickets`. |
| **Cách khắc phục** | • `dbt/macros/normalize_priority.sql`: Viết khối `CASE` chuẩn hóa 3 nhóm.<br>• `dbt/models/silver/silver_tickets.sql`: Lọc bản ghi hợp lệ (`priority_clean is not null`) trước khi đánh số thứ tự `row_number()`.<br>• `dbt/models/silver/quarantine_tickets.sql`: Lấy các hàng có macro trả về `NULL`.<br>• `dbt/models/silver/schema.yml`: Bật `enforced: true`, thêm test `not_null` và `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng (khớp kỳ vọng) · `silver_tickets.priority` sạch 100% (12,480 hàng ∈ 1..4) · `dbt test` = 11/11 pass. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> **Nên lưu trữ toàn bộ ở Bronze và lọc/quarantine ở Silver:** Tầng Bronze đóng vai trò là "nguồn chân lý" (Single Source of Truth) lưu vết trung thực dữ liệu thô từ nguồn. Nếu chặn ngay ở Bronze, ta sẽ mất dấu vết và không thể replay hoặc điều tra root cause khi source thay đổi format.
> **Không để pipeline dừng khi gặp bản ghi lỗi:** Trong môi trường vận hành thực tế, 312 bản ghi lỗi không thể làm gián đoạn việc phục vụ của hơn 130,000 event và 12,480 ticket hoàn toàn bình thường (tính sẵn sàng - Availability). Việc phân tuyến dữ liệu lỗi vào Quarantine Table giúp pipeline tiếp tục vận hành thông suốt cho downstream consumers, đồng thời tạo hàng đợi riêng để đội vận hành xử lý sau.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | Cả hai bài: **A & B** |
| **Nguyên nhân** | **Bài A:** 5,000 file Parquet nhỏ không được partition và predicate dạng non-sargable `strftime()` khiến DuckDB phải quét toàn bộ 5 triệu hàng.<br>**Bài B:** Consumer commit offset trước khi ghi dữ liệu (at-most-once) khiến khi crash bị mất dữ liệu; nếu ghi trước commit sau mà không có cơ chế dedup thì khi replay sẽ bị nhân bản dữ liệu. |
| **Cách khắc phục** | **Bài A:** Dùng `tools/compact.py` phân vùng Parquet theo `event_date`, sắp xếp theo `customer_name, event_time`, `row_group_size=500`. Sửa `queries/dashboard.sql` đọc với `hive_partitioning=true` và sargable filter `event_date = '2026-08-09'`.<br>**Bài B:** Trong `ingest/consumer.py`, chuyển sang ghi dữ liệu trước rồi mới commit offset (`at-least-once`), kết hợp DDL `PRIMARY KEY (event_id)` và `INSERT ... ON CONFLICT (event_id) DO NOTHING` để đạt `effective exactly-once`. |
| **Bằng chứng** | **Bài A:** `rows scanned` giảm từ 5,000,000 xuống **9,324** (giảm **536.3×**, mục tiêu ≥ 10×); số file giảm từ 5,000 xuống **14**; kết quả truy vấn hash giữ nguyên `4379e4c5d9f3`.<br>**Bài B:** `make crash-test` pass 100%: không mất bản ghi, không trùng bản ghi, C == A = 20,000 hàng. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra tính **Idempotency** của pipeline: cấu hình materialization (`unique_key`, `strategy`) và các tham số DAG (`catchup`, `max_active_runs`) khi chạy lại cùng một batch. |
| 2 | Đo đạc phân bố **độ trễ dữ liệu** (P95, P99) giữa event time và ingestion time để thiết kế **Lookback Window** và merge key phù hợp, tránh thất thoát late-arriving data. |
| 3 | Kiểm tra **Data Contract & Schema Validation** ở các tầng biến đổi, xây dựng cơ chế **Data Quarantine** để cách ly bản ghi lỗi mà không làm sập pipeline downstream. |
