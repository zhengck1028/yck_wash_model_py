# yck_car_basic_config 写入前数据验证规则（草案）

目的：写入 IT 生产库前拦截**字段错位**和**异常值**（如车长跑进 emission、
悬架跑进 abs、变速箱跑进 length）。规则基于**两个依据**：
1. **现有清洗代码的输出值域**（autohome_match / vdatabase_sync）——清洗后
   「应该」产出什么，验证就检查什么。这是主依据。
2. 真实库数据分位数分布（已排除手动录入 autohome_id>1111111000）——印证
   合理范围、发现历史脏数据模式。

清洗代码关键输出值域（本规则据此对齐）：
- 配置标志位（40+ 个 varchar(4)）：`_normalize_config_flag` 后**仅 {Y, N, -}**
  （标配→Y / 选配→- / 无→N / 前后主副-→N）
- gear_box：**原始变速箱描述**（5挡手动/CVT无级变速/-），不做四值归一
- emission：排量格式（2.0T/1.5L/-）
- environmental_standards：`_normalize_emission` 后为 国/欧+一~六（国六/欧五/国四/国五/-）
- is_green ∈ {0,1,2,3,4,5,6}；is_selling ∈ {在售,停售,即将销售}

实测到的历史错位污染（作为反向判据）：
- abs 等标志位里混入「多连杆式/承载式/双横臂式」→ 悬架错位
- fuel 里混入「盘式/鼓式/通风盘式」→ 制动错位
- environmental_standards 里混入「前置前驱」→ 驱动方式错位
- length 里混入「9挡手自一体/2.0T 224马力 L4」→ 变速箱/发动机错位
- emission 纯4位数 / gear_box 纯数字 / seat_number≥100 → 尺寸错位

分位数依据（p1~p99，主体正常范围）：

分位数依据（p1~p99，主体正常范围）：
- length 3460~5995 / width 1540~2098 / height 1325~2740 / wheelbase 2199~3760
- seat_number 2~10 / cylinders 3~8 / max_ps 69~549

校验分三级：
- **ERROR**（拦截，不写入该行）：铁定错位/非法，写入会污染生产库
- **WARN**（放行但告警）：可疑但可能是合法边缘值（商用车等）
- **长度**：超 varchar(N) 直接 ERROR（MySQL 会截断）

---

## 一、标识 / 关联字段

| 字段 | 类型 | 规则 | 级别 |
|---|---|---|---|
| autohome_id | int | 必填；`^[0-9]+$`；≤1111111000（>为手动录入，不应经此写入） | ERROR |
| brandid | int | 必填；`^[0-9]+$` | ERROR |
| series_id | int | 必填；`^[0-9]+$` | ERROR |
| mark | varchar(4) | `^[A-Z]$` 或 `-`（品牌首字母） | WARN |
| brand | varchar(200) | 非空、非 `-`；长度≤200 | ERROR |
| series | varchar(255) | 非空、非 `-`；长度≤255 | ERROR |
| type_name | varchar(255) | 非空；应含 series 前缀；长度≤255 | ERROR |

## 二、基本信息

| 字段 | 类型 | 规则 | 级别 |
|---|---|---|---|
| is_selling | varchar(10) | ∈ {在售, 停售, 即将销售}；`在售车型`等需先清洗 | ERROR |
| recommend_price | varchar(20) | `^[0-9]+(\.[0-9]+)?$`；0<price≤2000（万元） | ERROR |
| year | varchar(4) | `^(19|20)\d{2}$`；1990~次年+1 | ERROR |
| factory_name | varchar(50) | 长度≤50 | 长度 |
| produced_place | varchar(30) | 允许空；长度≤30 | 长度 |
| is_green | tinyint | ∈ {0,1,2,3,4,5,6} | ERROR |

## 三、车身尺寸（错位重灾区，重点校验）

| 字段 | 类型 | 规则 | 级别 |
|---|---|---|---|
| length | varchar(5) | `^[0-9]{3,4}$`；**含中文/字母=错位**；正常 2000~7000mm，主体 3460~5995 | ERROR（含非数字或超范围） |
| width | varchar(5) | `^[0-9]{3,4}$`；正常 1000~2500mm | ERROR |
| height | varchar(5) | `^[0-9]{3,4}$`；正常 1000~3200mm | ERROR |
| wheelbase | varchar(5) | `^[0-9]{3,5}$`；正常 1800~4200mm | ERROR |
| weight | varchar(5) | `^[0-9]{3,5}$`；正常 500~5000kg | WARN（商用车偏大） |
| seat_number | varchar(20) | `^[0-9]{1,2}$`；正常 2~10；**≥100=错位**；11~50 为客车 WARN | ERROR（≥100 或非数字）/ WARN（11~50） |
| body_structure | varchar(30) | 应含「车身结构」类词（三厢/两厢/SUV/MPV/…）；长度≤30 | WARN |
| trunk_volume | varchar(20) | 允许空/范围值（如 427-1263）；长度≤20 | 长度 |

## 四、发动机 / 动力

| 字段 | 类型 | 规则 | 级别 |
|---|---|---|---|
| emission | varchar(10) | 排量格式 `\d\.\d[TL]?` 或车型等级或 `-`；**纯4位数=车长错位** | ERROR（纯4位数） |
| engine | varchar(50) | 长度≤50 | 长度 |
| gear_box | varchar(50) | 变速箱描述（含 挡/自动/手动/CVT/双离合/DHT/电动）或 `-`；**纯数字=车宽错位** | ERROR（纯数字） |
| intake | varchar(30) | 进气形式（自然吸气/涡轮增压/…）；长度≤30 | WARN |
| cylinder_arrangement | varchar(4) | L/V/H/W + 或 `-`；长度≤4 | 长度 |
| cylinders | varchar(4) | `^[0-9]{1,2}$`；正常 2~16 | WARN |
| max_ps | varchar(5) | `^[0-9]{1,4}$`；正常 20~1500 | WARN |
| max_n-m | varchar(5) | `^[0-9]{1,4}$` 或 `-` | WARN |
| fuel | varchar(10) | ∈ {汽油,柴油,纯电动,油电混合,插电式混合动力,增程式,汽油+48V轻混系统,CNG,…} 或 `-`；**出现「盘式/鼓式/通风盘式」=刹车错位** | ERROR（命中刹车词） |
| fuel_grade | varchar(30) | 号数（92号/95号/0号/…）或 `-`；长度≤30 | WARN |
| fuel_supply_system | varchar(20) | 供油方式（直喷/多点电喷/…）；长度≤20 | 长度 |
| environmental_standards_org | varchar(20) | 排放原文；长度≤20 | 长度 |
| environmental_standards | varchar(20) | 规范化后仅 `(国|欧|京)(一~六)` 组合或 `-`（国六/欧五/国四/国五）；**出现「前置前驱」等驱动词=错位** | ERROR（命中驱动词） |

## 五、底盘 / 制动 / 轮胎

| 字段 | 类型 | 规则 | 级别 |
|---|---|---|---|
| drive_mode | varchar(20) | 驱动形式（前置前驱/前置四驱/…）；长度≤20 | WARN |
| front/rear_suspension_type | varchar(50) | 悬架类型（麦弗逊/多连杆/双叉臂/…）；长度≤50 | 长度 |
| power_type | varchar(30) | 长度≤30 | 长度 |
| car_boty_type | varchar(30) | 车体类型；长度≤30 | 长度 |
| front/rear_brake_type | varchar(30) | 制动类型（通风盘式/盘式/鼓式）；长度≤30 | 长度 |
| parking_brake_type | varchar(30) | 驻车方式（电子驻车/机械手刹/…）；长度≤30 | 长度 |
| front/rear_tire_specifications | varchar(30) | 轮胎规格 `\d+/\d+\s*R\d+`；**出现「标配/选配/驻车」=错位** | ERROR（命中配置词） |

## 六、配置标志位（40+ 字段，最强错位判据）

`abs, master_air_bag, sub_air_bag, front/rear_side_air_bag, front/rear_head_air_bag,
tire_pressure_monitoring, ISOFIX_child_seat_interfaces, car_internal_lock,
keyless_start_system, brake_assist, stability_control, electric_roof, panoramic_roof,
multifunction_steering_wheel, cruise, front/rear_parking_radar, reverse_video_image,
electric_seat_memory, front/rear_seat_heating, front/rear_seat_ventilation, gps,
daytime_running_light, automatic_headlights, front_fog_lamp, front/rear_electric_window,
mirror_electric_adjustment, mirror_heating` —— 均 varchar(4)

| 规则 | 级别 |
|---|---|
| 取值必须 **严格 ∈ {Y, N, -}**（`_normalize_config_flag` 清洗后只产出这三值；空串也应被清成 N） | ERROR |
| **出现「多连杆/承载式/双横臂/悬架/盘式/鼓式」等非标值 = 悬架/制动错位** | ERROR |
| Y/Y、Y/N 这类未拆分值 → 应已被 _split_pair 处理，残留则 WARN | WARN |

> 例外（非 Y/N 的枚举字段，单列规则）：
> - variable_suspension varchar(30)：可为具体描述或 Y/N/- ；长度≤30
> - seat_material varchar(20)：座椅材质（真皮/仿皮/织物/…）；长度≤20
> - low_beam_lamp varchar(30)：近光灯光源（LED/卤素/…）；长度≤30
> - air_conditioning_control_mode varchar(30)：空调控制方式；长度≤30

## 七、颜色 / 配置串

| 字段 | 类型 | 规则 | 级别 |
|---|---|---|---|
| color_outside | text | 颜色名，分号分隔；不应含数字/无 | WARN |
| color_outside_code | text | 颜色代码；不应残留「无」 | WARN |
| hl_configs | varchar(200) | 亮点配置串（分号分隔）；长度≤200 | 长度 |
| hl_configc | varchar(200) | 长度≤200 | 长度 |

---

## 行级错位综合判据（任一命中即高度疑似整行错位 → ERROR）

1. `length` 含中文或字母（如「9挡手自一体」「2.0T 224马力 L4」）
2. `emission` 为纯 4 位数字（车长错位进来）
3. `fuel` 命中刹车词（盘式/鼓式/通风盘式）
4. `gear_box` 为纯数字（车宽错位）
5. 任一配置标志位（Y/N/- 字段）含悬架/制动词（多连杆/承载式/双横臂/盘式/鼓式）
6. `seat_number` ≥ 100（尺寸错位进来）
7. `front_tire_specifications` 命中配置词（标配/选配/驻车）
8. `environmental_standards` 命中驱动词（前置前驱/四驱/两驱）

命中 ≥2 条 → 铁定错位；命中 1 条 → 强疑，ERROR 拦截。

---

## 待你确认的点

1. **ERROR 行为**：命中 ERROR 的行是「整行剔除不写」还是「整批中止」？建议剔除+汇总报告。
2. **WARN 行为**：放行 + 记录到报告，还是也剔除？
3. **范围阈值**：上面的合理范围（如 length 2000~7000）是否符合业务？商用车/客车是否要放宽？
4. **规则存放形式**：确认后我建议落成 `common/validation.py`（一个 validate(df)→报告 的函数），供 resync 脚本和 autohome_match 写入前调用。
