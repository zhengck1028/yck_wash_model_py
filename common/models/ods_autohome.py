"""自动生成的 ORM 模型——请勿手改，改 tools/gen_orm_models.py 后重新生成。"""
from typing import Optional
import datetime
import decimal

from sqlalchemy import BigInteger, CHAR, DECIMAL, DateTime, Integer, String, Text, text
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class ConfigAutohomeDetailInfo(Base):
    __tablename__ = 'config_autohome_detail_info'
    __table_args__ = {'comment': 'ODS - 汽车之家详细配置'}

    autohome_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, comment='汽车之家车型ID')
    emission: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"), comment='排量')
    level: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"), comment='车级')
    engine: Mapped[Optional[str]] = mapped_column(String(100), server_default=text("'-'"), comment='发动机')
    gear_box: Mapped[Optional[str]] = mapped_column(String(100), server_default=text("'-'"), comment='变速箱')
    length: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"), comment='车长(mm)')
    width: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"), comment='车宽(mm)')
    height: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"), comment='车高(mm)')
    body_structure: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"), comment='车身结构')
    seat_number: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"), comment='座位数')
    wheelbase: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"), comment='轴距(mm)')
    weight: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"), comment='整备质量(kg)')
    trunk_volume: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"), comment='行李箱容积(L)')
    intake: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"), comment='进气形式')
    cylinder_arrangement: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"), comment='气缸排列')
    cylinders: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"), comment='气缸数')
    admission_gear: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"), comment='配气机构')
    max_ps: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"), comment='最大功率(PS)')
    max_n_m: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"), comment='最大扭矩(N·m)')
    fuel: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"), comment='燃料类型')
    fuel_grade: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"), comment='燃油标号')
    fuel_supply_system: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"), comment='供油方式')
    environmental_standards_org: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"), comment='排放标准原始值')
    drive_mode: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"), comment='驱动方式')
    front_suspension_type: Mapped[Optional[str]] = mapped_column(String(100), server_default=text("'-'"), comment='前悬架类型')
    rear_suspension_type: Mapped[Optional[str]] = mapped_column(String(100), server_default=text("'-'"), comment='后悬架类型')
    power_type: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"), comment='动力类型')
    car_boty_type: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"), comment='车身类型')
    front_brake_type: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"), comment='前制动器类型')
    rear_brake_type: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"), comment='后制动器类型')
    parking_brake_type: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"), comment='驻车制动类型')
    front_tire_specifications: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"), comment='前轮胎规格')
    rear_tire_specifications: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"), comment='后轮胎规格')
    master_air_bag: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    sub_air_bag: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    front_side_air_bag: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    rear_side_air_bag: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    front_head_air_bag: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    rear_head_air_bag: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    tire_pressure_monitoring: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    ISOFIX_child_seat_interfaces: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    car_internal_lock: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    keyless_start_system: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    abs: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    brake_assist: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    stability_control: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    variable_suspension: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    electric_roof: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    panoramic_roof: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    multifunction_steering_wheel: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    cruise: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    front_parking_radar: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    rear_parking_radar: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    reverse_video_image: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    seat_material: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"))
    electric_seat_memory: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    front_seat_heating: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    rear_seat_heating: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    front_seat_ventilation: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    rear_seat_ventilation: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    gps: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    low_beam_lamp: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"))
    daytime_running_light: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    automatic_headlights: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    front_fog_lamp: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    front_electric_window: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    rear_electric_window: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    mirror_electric_adjustment: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    mirror_heating: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    air_conditioning_control_mode: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"))
    color_outside: Mapped[Optional[str]] = mapped_column(Text, comment='外观颜色（分号分隔）')
    color_outside_code: Mapped[Optional[str]] = mapped_column(Text, comment='颜色代码（分号分隔）')
    url: Mapped[Optional[str]] = mapped_column(String(500))
    update_time: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)


class ConfigAutohomeEvInfo(Base):
    __tablename__ = 'config_autohome_ev_info'
    __table_args__ = {'comment': 'ODS - 汽车之家新能源参数'}

    model_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, comment='汽车之家车型ID')
    ee_type: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"))
    ee_total_power: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"))
    ee_total_torque: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"))
    ee_front_max_kw: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"))
    ee_front_max_Nm: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"))
    ee_after_max_kw: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"))
    ee_after_max_Nm: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"))
    ee_system_integrated_kw: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"))
    ee_system_integrated_Nm: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"))
    ee_driving_number: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"))
    ee_layout: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"))
    ee_battery_type: Mapped[Optional[str]] = mapped_column(String(100), server_default=text("'-'"))
    ee_pure_electric_range: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"))
    ee_battery_capacity: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"))
    ee_power_consumption: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"))
    ee_battery_quality_assurance: Mapped[Optional[str]] = mapped_column(String(100), server_default=text("'-'"))
    ee_fast_charging_time: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"))
    ee_slow_charging_time: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"))
    ee_fast_charge: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'-'"))
    update_time: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    add_time: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)


class ConfigAutohomeMajorInfoTmp(Base):
    __tablename__ = 'config_autohome_major_info_tmp'
    __table_args__ = {'comment': 'ODS - 汽车之家车型主表'}

    model_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, comment='汽车之家车型ID')
    brandid: Mapped[Optional[int]] = mapped_column(Integer)
    brand_name: Mapped[Optional[str]] = mapped_column(String(100))
    Initial: Mapped[Optional[str]] = mapped_column(CHAR(1))
    series_group_name: Mapped[Optional[str]] = mapped_column(String(100))
    series_id: Mapped[Optional[int]] = mapped_column(Integer)
    series_name: Mapped[Optional[str]] = mapped_column(String(100))
    model_year: Mapped[Optional[str]] = mapped_column(String(10))
    model_name: Mapped[Optional[str]] = mapped_column(String(200))
    model_price: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(10, 2), server_default=text("'0.00'"))
    status: Mapped[Optional[str]] = mapped_column(String(50))
    liter: Mapped[Optional[str]] = mapped_column(String(20), server_default=text("'-'"))
    auto: Mapped[Optional[str]] = mapped_column(String(200))
    discharge_standard: Mapped[Optional[str]] = mapped_column(String(50))
    is_green: Mapped[Optional[int]] = mapped_column(TINYINT, server_default=text("'0'"))
    is_check: Mapped[Optional[int]] = mapped_column(TINYINT, server_default=text("'0'"), comment='审核通过后可用于匹配')


class ConfigAutohomeYckBrand(Base):
    __tablename__ = 'config_autohome_yck_brand'
    __table_args__ = {'comment': 'ODS - 汽车之家品牌（YCK标准）'}

    brandid: Mapped[int] = mapped_column(Integer, primary_key=True, comment='汽车之家品牌ID')
    Initial: Mapped[Optional[str]] = mapped_column(CHAR(1), comment='首字母')
    brand_name: Mapped[Optional[str]] = mapped_column(String(100), comment='品牌名称')
    car_country: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'无'"), comment='产地')


class ConfigAutohomeYckSeries(Base):
    __tablename__ = 'config_autohome_yck_series'
    __table_args__ = {'comment': 'ODS - 汽车之家车系（YCK标准）'}

    series_id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='汽车之家车系ID')
    series_group_name: Mapped[Optional[str]] = mapped_column(String(100), comment='厂商名称')
    series_name: Mapped[Optional[str]] = mapped_column(String(100), comment='车系名称')
    brandid: Mapped[Optional[int]] = mapped_column(Integer)
    brand_name: Mapped[Optional[str]] = mapped_column(String(100))
    car_level: Mapped[Optional[str]] = mapped_column(String(50), server_default=text("'无'"), comment='车级')
    is_import: Mapped[Optional[int]] = mapped_column(TINYINT, server_default=text("'0'"), comment='是否进口')
