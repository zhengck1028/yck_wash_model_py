-- ============================================================
-- 创建 ODS 层数据库：yck_ods
-- 服务器：DB_LOCAL 192.168.0.10
-- 用途：vdatabase_sync ETL 后的车型配置中间层
--       供 autohome_match 读取后写入 IT 业务库
-- ============================================================

CREATE DATABASE IF NOT EXISTS `yck_ods`
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_general_ci;

USE `yck_ods`;

-- ── 1. 新能源参数表（车300） ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS `config_che300_ev_info` (
  `model_id`                     BIGINT       NOT NULL COMMENT '车300车型ID',
  `ee_type`                      VARCHAR(50)  DEFAULT '-' COMMENT '驱动类型',
  `ee_total_power`               VARCHAR(50)  DEFAULT '-' COMMENT '系统总功率(kW)',
  `ee_total_torque`              VARCHAR(50)  DEFAULT '-' COMMENT '系统总扭矩(N·m)',
  `ee_front_max_kw`              VARCHAR(50)  DEFAULT '-' COMMENT '前电机最大功率(kW)',
  `ee_front_max_Nm`              VARCHAR(50)  DEFAULT '-' COMMENT '前电机最大扭矩(N·m)',
  `ee_after_max_kw`              VARCHAR(50)  DEFAULT '-' COMMENT '后电机最大功率(kW)',
  `ee_after_max_Nm`              VARCHAR(50)  DEFAULT '-' COMMENT '后电机最大扭矩(N·m)',
  `ee_system_integrated_kw`      VARCHAR(50)  DEFAULT '-' COMMENT '电机综合功率(kW)',
  `ee_system_integrated_Nm`      VARCHAR(50)  DEFAULT '-' COMMENT '电机综合扭矩(N·m)',
  `ee_driving_number`            VARCHAR(50)  DEFAULT '-' COMMENT '电动机数量',
  `ee_layout`                    VARCHAR(50)  DEFAULT '-' COMMENT '电机布局',
  `ee_battery_type`              VARCHAR(100) DEFAULT '-' COMMENT '电池类型',
  `ee_pure_electric_range`       VARCHAR(50)  DEFAULT '-' COMMENT '纯电续航(km)',
  `ee_battery_capacity`          VARCHAR(50)  DEFAULT '-' COMMENT '电池容量(kWh)',
  `ee_power_consumption`         VARCHAR(50)  DEFAULT '-' COMMENT '百公里电耗(kWh)',
  `ee_battery_quality_assurance` VARCHAR(100) DEFAULT '-' COMMENT '电池质保',
  `ee_fast_charging_time`        VARCHAR(50)  DEFAULT '-' COMMENT '快充时间',
  `ee_slow_charging_time`        VARCHAR(50)  DEFAULT '-' COMMENT '慢充时间',
  `ee_fast_charge`               VARCHAR(50)  DEFAULT '-' COMMENT '快充电量(%)',
  `update_time`                  DATETIME     DEFAULT NULL,
  `add_time`                     DATETIME     DEFAULT NULL,
  PRIMARY KEY (`model_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS - 车300新能源参数';

-- ── 2. 新能源参数表（汽车之家） ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS `config_autohome_ev_info` (
  `model_id`                     BIGINT       NOT NULL COMMENT '汽车之家车型ID',
  `ee_type`                      VARCHAR(50)  DEFAULT '-',
  `ee_total_power`               VARCHAR(50)  DEFAULT '-',
  `ee_total_torque`              VARCHAR(50)  DEFAULT '-',
  `ee_front_max_kw`              VARCHAR(50)  DEFAULT '-',
  `ee_front_max_Nm`              VARCHAR(50)  DEFAULT '-',
  `ee_after_max_kw`              VARCHAR(50)  DEFAULT '-',
  `ee_after_max_Nm`              VARCHAR(50)  DEFAULT '-',
  `ee_system_integrated_kw`      VARCHAR(50)  DEFAULT '-',
  `ee_system_integrated_Nm`      VARCHAR(50)  DEFAULT '-',
  `ee_driving_number`            VARCHAR(50)  DEFAULT '-',
  `ee_layout`                    VARCHAR(50)  DEFAULT '-',
  `ee_battery_type`              VARCHAR(100) DEFAULT '-',
  `ee_pure_electric_range`       VARCHAR(50)  DEFAULT '-',
  `ee_battery_capacity`          VARCHAR(50)  DEFAULT '-',
  `ee_power_consumption`         VARCHAR(50)  DEFAULT '-',
  `ee_battery_quality_assurance` VARCHAR(100) DEFAULT '-',
  `ee_fast_charging_time`        VARCHAR(50)  DEFAULT '-',
  `ee_slow_charging_time`        VARCHAR(50)  DEFAULT '-',
  `ee_fast_charge`               VARCHAR(50)  DEFAULT '-',
  `update_time`                  DATETIME     DEFAULT NULL,
  `add_time`                     DATETIME     DEFAULT NULL,
  PRIMARY KEY (`model_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS - 汽车之家新能源参数';

-- ── 3. 车300详细配置表 ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS `config_che300_detail_info` (
  `model_id`                      BIGINT       NOT NULL,
  `master_air_bag`                VARCHAR(20)  DEFAULT '-',
  `sub_air_bag`                   VARCHAR(20)  DEFAULT '-',
  `front_side_air_bag`            VARCHAR(20)  DEFAULT '-',
  `rear_side_air_bag`             VARCHAR(20)  DEFAULT '-',
  `front_head_air_bag`            VARCHAR(20)  DEFAULT '-',
  `rear_head_air_bag`             VARCHAR(20)  DEFAULT '-',
  `tire_pressure_monitoring`      VARCHAR(20)  DEFAULT '-',
  `ISOFIX_child_seat_interfaces`  VARCHAR(20)  DEFAULT '-',
  `car_internal_lock`             VARCHAR(20)  DEFAULT '-',
  `keyless_start_system`          VARCHAR(20)  DEFAULT '-',
  `abs`                           VARCHAR(20)  DEFAULT '-',
  `brake_assist`                  VARCHAR(20)  DEFAULT '-',
  `stability_control`             VARCHAR(20)  DEFAULT '-',
  `variable_suspension`           VARCHAR(20)  DEFAULT '-',
  `electric_roof`                 VARCHAR(20)  DEFAULT '-',
  `panoramic_roof`                VARCHAR(20)  DEFAULT '-',
  `multifunction_steering_wheel`  VARCHAR(20)  DEFAULT '-',
  `cruise`                        VARCHAR(20)  DEFAULT '-',
  `front_parking_radar`           VARCHAR(20)  DEFAULT '-',
  `rear_parking_radar`            VARCHAR(20)  DEFAULT '-',
  `reverse_video_image`           VARCHAR(20)  DEFAULT '-',
  `seat_material`                 VARCHAR(50)  DEFAULT '-',
  `electric_seat_memory`          VARCHAR(20)  DEFAULT '-',
  `front_seat_heating`            VARCHAR(20)  DEFAULT '-',
  `rear_seat_heating`             VARCHAR(20)  DEFAULT '-',
  `front_seat_ventilation`        VARCHAR(20)  DEFAULT '-',
  `rear_seat_ventilation`         VARCHAR(20)  DEFAULT '-',
  `gps`                           VARCHAR(20)  DEFAULT '-',
  `low_beam_lamp`                 VARCHAR(50)  DEFAULT '-',
  `daytime_running_light`         VARCHAR(20)  DEFAULT '-',
  `automatic_headlights`          VARCHAR(20)  DEFAULT '-',
  `front_fog_lamp`                VARCHAR(20)  DEFAULT '-',
  `front_electric_window`         VARCHAR(20)  DEFAULT '-',
  `rear_electric_window`          VARCHAR(20)  DEFAULT '-',
  `mirror_electric_adjustment`    VARCHAR(20)  DEFAULT '-',
  `mirror_heating`                VARCHAR(20)  DEFAULT '-',
  `air_conditioning_control_mode` VARCHAR(50)  DEFAULT '-',
  `update_time`                   DATETIME     DEFAULT NULL,
  PRIMARY KEY (`model_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS - 车300详细配置';

-- ── 4. 汽车之家详细配置表 ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS `config_autohome_detail_info` (
  `autohome_id`                   BIGINT       NOT NULL COMMENT '汽车之家车型ID',
  `emission`                      VARCHAR(20)  DEFAULT '-' COMMENT '排量',
  `level`                         VARCHAR(50)  DEFAULT '-' COMMENT '车级',
  `engine`                        VARCHAR(100) DEFAULT '-' COMMENT '发动机',
  `gear_box`                      VARCHAR(100) DEFAULT '-' COMMENT '变速箱',
  `length`                        VARCHAR(20)  DEFAULT '-' COMMENT '车长(mm)',
  `width`                         VARCHAR(20)  DEFAULT '-' COMMENT '车宽(mm)',
  `height`                        VARCHAR(20)  DEFAULT '-' COMMENT '车高(mm)',
  `body_structure`                VARCHAR(50)  DEFAULT '-' COMMENT '车身结构',
  `seat_number`                   VARCHAR(20)  DEFAULT '-' COMMENT '座位数',
  `wheelbase`                     VARCHAR(20)  DEFAULT '-' COMMENT '轴距(mm)',
  `weight`                        VARCHAR(20)  DEFAULT '-' COMMENT '整备质量(kg)',
  `trunk_volume`                  VARCHAR(20)  DEFAULT '-' COMMENT '行李箱容积(L)',
  `intake`                        VARCHAR(50)  DEFAULT '-' COMMENT '进气形式',
  `cylinder_arrangement`          VARCHAR(50)  DEFAULT '-' COMMENT '气缸排列',
  `cylinders`                     VARCHAR(20)  DEFAULT '-' COMMENT '气缸数',
  `admission_gear`                VARCHAR(50)  DEFAULT '-' COMMENT '配气机构',
  `max_ps`                        VARCHAR(20)  DEFAULT '-' COMMENT '最大功率(PS)',
  `max_n_m`                       VARCHAR(20)  DEFAULT '-' COMMENT '最大扭矩(N·m)',
  `fuel`                          VARCHAR(50)  DEFAULT '-' COMMENT '燃料类型',
  `fuel_grade`                    VARCHAR(50)  DEFAULT '-' COMMENT '燃油标号',
  `fuel_supply_system`            VARCHAR(50)  DEFAULT '-' COMMENT '供油方式',
  `environmental_standards_org`   VARCHAR(50)  DEFAULT '-' COMMENT '排放标准原始值',
  `drive_mode`                    VARCHAR(50)  DEFAULT '-' COMMENT '驱动方式',
  `front_suspension_type`         VARCHAR(100) DEFAULT '-' COMMENT '前悬架类型',
  `rear_suspension_type`          VARCHAR(100) DEFAULT '-' COMMENT '后悬架类型',
  `power_type`                    VARCHAR(50)  DEFAULT '-' COMMENT '动力类型',
  `car_boty_type`                 VARCHAR(50)  DEFAULT '-' COMMENT '车身类型',
  `front_brake_type`              VARCHAR(50)  DEFAULT '-' COMMENT '前制动器类型',
  `rear_brake_type`               VARCHAR(50)  DEFAULT '-' COMMENT '后制动器类型',
  `parking_brake_type`            VARCHAR(50)  DEFAULT '-' COMMENT '驻车制动类型',
  `front_tire_specifications`     VARCHAR(50)  DEFAULT '-' COMMENT '前轮胎规格',
  `rear_tire_specifications`      VARCHAR(50)  DEFAULT '-' COMMENT '后轮胎规格',
  `master_air_bag`                VARCHAR(20)  DEFAULT '-',
  `sub_air_bag`                   VARCHAR(20)  DEFAULT '-',
  `front_side_air_bag`            VARCHAR(20)  DEFAULT '-',
  `rear_side_air_bag`             VARCHAR(20)  DEFAULT '-',
  `front_head_air_bag`            VARCHAR(20)  DEFAULT '-',
  `rear_head_air_bag`             VARCHAR(20)  DEFAULT '-',
  `tire_pressure_monitoring`      VARCHAR(20)  DEFAULT '-',
  `ISOFIX_child_seat_interfaces`  VARCHAR(20)  DEFAULT '-',
  `car_internal_lock`             VARCHAR(20)  DEFAULT '-',
  `keyless_start_system`          VARCHAR(20)  DEFAULT '-',
  `abs`                           VARCHAR(20)  DEFAULT '-',
  `brake_assist`                  VARCHAR(20)  DEFAULT '-',
  `stability_control`             VARCHAR(20)  DEFAULT '-',
  `variable_suspension`           VARCHAR(20)  DEFAULT '-',
  `electric_roof`                 VARCHAR(20)  DEFAULT '-',
  `panoramic_roof`                VARCHAR(20)  DEFAULT '-',
  `multifunction_steering_wheel`  VARCHAR(20)  DEFAULT '-',
  `cruise`                        VARCHAR(20)  DEFAULT '-',
  `front_parking_radar`           VARCHAR(20)  DEFAULT '-',
  `rear_parking_radar`            VARCHAR(20)  DEFAULT '-',
  `reverse_video_image`           VARCHAR(20)  DEFAULT '-',
  `seat_material`                 VARCHAR(50)  DEFAULT '-',
  `electric_seat_memory`          VARCHAR(20)  DEFAULT '-',
  `front_seat_heating`            VARCHAR(20)  DEFAULT '-',
  `rear_seat_heating`             VARCHAR(20)  DEFAULT '-',
  `front_seat_ventilation`        VARCHAR(20)  DEFAULT '-',
  `rear_seat_ventilation`         VARCHAR(20)  DEFAULT '-',
  `gps`                           VARCHAR(20)  DEFAULT '-',
  `low_beam_lamp`                 VARCHAR(50)  DEFAULT '-',
  `daytime_running_light`         VARCHAR(20)  DEFAULT '-',
  `automatic_headlights`          VARCHAR(20)  DEFAULT '-',
  `front_fog_lamp`                VARCHAR(20)  DEFAULT '-',
  `front_electric_window`         VARCHAR(20)  DEFAULT '-',
  `rear_electric_window`          VARCHAR(20)  DEFAULT '-',
  `mirror_electric_adjustment`    VARCHAR(20)  DEFAULT '-',
  `mirror_heating`                VARCHAR(20)  DEFAULT '-',
  `air_conditioning_control_mode` VARCHAR(50)  DEFAULT '-',
  `color_outside`                 TEXT         DEFAULT NULL COMMENT '外观颜色（分号分隔）',
  `color_outside_code`            TEXT         DEFAULT NULL COMMENT '颜色代码（分号分隔）',
  `url`                           VARCHAR(500) DEFAULT NULL,
  `update_time`                   DATETIME     DEFAULT NULL,
  PRIMARY KEY (`autohome_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS - 汽车之家详细配置';

-- ── 5. 汽车之家品牌标准表 ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS `config_autohome_yck_brand` (
  `brandid`     INT          NOT NULL COMMENT '汽车之家品牌ID',
  `Initial`     CHAR(1)      DEFAULT NULL COMMENT '首字母',
  `brand_name`  VARCHAR(100) DEFAULT NULL COMMENT '品牌名称',
  `car_country` VARCHAR(50)  DEFAULT '无' COMMENT '产地',
  PRIMARY KEY (`brandid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS - 汽车之家品牌（YCK标准）';

-- ── 6. 汽车之家车系标准表 ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS `config_autohome_yck_series` (
  `series_id`         INT          NOT NULL COMMENT '汽车之家车系ID',
  `series_group_name` VARCHAR(100) DEFAULT NULL COMMENT '厂商名称',
  `series_name`       VARCHAR(100) DEFAULT NULL COMMENT '车系名称',
  `brandid`           INT          DEFAULT NULL,
  `brand_name`        VARCHAR(100) DEFAULT NULL,
  `car_level`         VARCHAR(50)  DEFAULT '无' COMMENT '车级',
  `is_import`         TINYINT      DEFAULT 0 COMMENT '是否进口',
  PRIMARY KEY (`series_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS - 汽车之家车系（YCK标准）';

-- ── 7. 车300主表 ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS `config_che300_major_info` (
  `model_id`           BIGINT        NOT NULL COMMENT '车300车型ID',
  `Initial`            CHAR(1)       DEFAULT NULL,
  `brandid`            INT           DEFAULT NULL,
  `brand_name`         VARCHAR(100)  DEFAULT NULL,
  `series_group_name`  VARCHAR(100)  DEFAULT NULL,
  `series_id`          INT           DEFAULT NULL,
  `series_name`        VARCHAR(100)  DEFAULT NULL,
  `model_name`         VARCHAR(200)  DEFAULT NULL,
  `model_price`        DECIMAL(10,2) DEFAULT 0,
  `model_year`         VARCHAR(10)   DEFAULT NULL,
  `auto`               VARCHAR(50)   DEFAULT '-',
  `liter`              VARCHAR(20)   DEFAULT '-',
  `liter_type`         VARCHAR(20)   DEFAULT '-',
  `gear_type`          VARCHAR(50)   DEFAULT '-',
  `discharge_standard` VARCHAR(20)   DEFAULT '-',
  `max_reg_year`       VARCHAR(10)   DEFAULT NULL,
  `min_reg_year`       VARCHAR(10)   DEFAULT NULL,
  `car_level`          VARCHAR(50)   DEFAULT NULL,
  `seat_number`        VARCHAR(10)   DEFAULT NULL,
  `short_name`         VARCHAR(100)  DEFAULT NULL,
  `hl_configs`         TEXT          DEFAULT NULL,
  `hl_configc`         TEXT          DEFAULT NULL,
  `is_green`           TINYINT       DEFAULT 0,
  `update_time`        DATETIME      DEFAULT NULL,
  PRIMARY KEY (`model_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS - 车300车型主表';

-- ── 8. 汽车之家主表（ETL后） ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS `config_autohome_major_info_tmp` (
  `model_id`           BIGINT        NOT NULL COMMENT '汽车之家车型ID',
  `brandid`            INT           DEFAULT NULL,
  `brand_name`         VARCHAR(100)  DEFAULT NULL,
  `Initial`            CHAR(1)       DEFAULT NULL,
  `series_group_name`  VARCHAR(100)  DEFAULT NULL,
  `series_id`          INT           DEFAULT NULL,
  `series_name`        VARCHAR(100)  DEFAULT NULL,
  `model_year`         VARCHAR(10)   DEFAULT NULL,
  `model_name`         VARCHAR(200)  DEFAULT NULL,
  `model_price`        DECIMAL(10,2) DEFAULT 0,
  `status`             VARCHAR(50)   DEFAULT NULL COMMENT '在售/停售',
  `liter`              VARCHAR(20)   DEFAULT '-',
  `auto`               VARCHAR(200)  DEFAULT '-',
  `discharge_standard` VARCHAR(50)   DEFAULT '-',
  `is_green`           TINYINT       DEFAULT 0,
  `is_check`           TINYINT       DEFAULT 0 COMMENT '审核通过后可用于匹配',
  PRIMARY KEY (`model_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS - 汽车之家车型主表';

-- ── 9. 车主之家主表（ETL后） ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS `config_autoowner_major_info_tmp` (
  `model_id`           BIGINT        NOT NULL,
  `brandid`            INT           DEFAULT NULL,
  `brand_name`         VARCHAR(100)  DEFAULT NULL,
  `Initial`            CHAR(1)       DEFAULT NULL,
  `series_group_name`  VARCHAR(100)  DEFAULT NULL,
  `series_id`          INT           DEFAULT NULL,
  `series_name`        VARCHAR(100)  DEFAULT NULL,
  `model_year`         INT           DEFAULT 0,
  `model_name`         VARCHAR(200)  DEFAULT NULL,
  `model_price`        DECIMAL(10,2) DEFAULT 0,
  `liter`              VARCHAR(20)   DEFAULT '-',
  `auto`               VARCHAR(50)   DEFAULT '-',
  `discharge_standard` VARCHAR(20)   DEFAULT '-',
  PRIMARY KEY (`model_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS - 车主之家车型主表';

-- ── 10. 汽车之家颜色表 ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS `config_autohome_color` (
  `id`                 BIGINT       NOT NULL AUTO_INCREMENT,
  `series_id`          INT          DEFAULT NULL COMMENT '车系ID',
  `color_outside`      TEXT         DEFAULT NULL COMMENT '颜色名称（分号分隔）',
  `color_outside_code` TEXT         DEFAULT NULL COMMENT '颜色代码（分号分隔）',
  `update_time`        DATETIME     DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_series_id` (`series_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS - 汽车之家外观颜色';
