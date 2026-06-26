"""自动生成的 ORM 模型——请勿手改，改 tools/gen_orm_models.py 后重新生成。"""
from typing import Optional

from sqlalchemy import Index, String, Text
from sqlalchemy.dialects.mysql import INTEGER, MEDIUMINT, TINYINT
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class YckCarBasicBrand(Base):
    __tablename__ = 'yck_car_basic_brand'

    brandid: Mapped[int] = mapped_column(INTEGER(15), primary_key=True)
    Initial: Mapped[str] = mapped_column(String(5), nullable=False)
    brand_name: Mapped[str] = mapped_column(String(30), nullable=False, comment='品牌')
    car_country: Mapped[str] = mapped_column(String(20), nullable=False, comment='国家')


class YckCarBasicConfig(Base):
    __tablename__ = 'yck_car_basic_config'
    __table_args__ = (
        Index('brand', 'brand'),
        Index('id', 'id', unique=True),
        Index('series', 'series'),
        Index('type_name', 'type_name')
    )

    id: Mapped[int] = mapped_column(MEDIUMINT(6), primary_key=True, autoincrement=True)
    autohome_id: Mapped[Optional[int]] = mapped_column(INTEGER(15), comment='汽车之家车型ID')
    mark: Mapped[Optional[str]] = mapped_column(String(4), comment='首字母')
    brandid: Mapped[Optional[int]] = mapped_column(INTEGER(15))
    brand: Mapped[Optional[str]] = mapped_column(String(200), comment='品牌')
    series_id: Mapped[Optional[int]] = mapped_column(INTEGER(15))
    series: Mapped[Optional[str]] = mapped_column(String(255), comment='车系')
    is_selling: Mapped[Optional[str]] = mapped_column(String(10), comment='销售状态')
    type_name: Mapped[Optional[str]] = mapped_column(String(255), comment='车型名称')
    recommend_price: Mapped[Optional[str]] = mapped_column(String(20), comment='厂商指导价(元)')
    year: Mapped[Optional[str]] = mapped_column(String(4), comment='年款')
    factory_name: Mapped[Optional[str]] = mapped_column(String(50), comment='厂商')
    produced_place: Mapped[Optional[str]] = mapped_column(String(30), comment='产地')
    is_green: Mapped[Optional[int]] = mapped_column(TINYINT(2), comment='动力类型(0燃油车:含汽油,柴油;1纯电动;2油电混合;3插电式;4增程式;5轻混;6氢燃料)')
    emission: Mapped[Optional[str]] = mapped_column(String(10), comment='排量')
    level: Mapped[Optional[str]] = mapped_column(String(20), comment='级别')
    engine: Mapped[Optional[str]] = mapped_column(String(50), comment='发动机')
    gear_box: Mapped[Optional[str]] = mapped_column(String(50), comment='变速箱')
    length: Mapped[Optional[str]] = mapped_column(String(5), comment='长')
    width: Mapped[Optional[str]] = mapped_column(String(5), comment='宽')
    height: Mapped[Optional[str]] = mapped_column(String(5), comment='高')
    body_structure: Mapped[Optional[str]] = mapped_column(String(30), comment='车身结构')
    seat_number: Mapped[Optional[str]] = mapped_column(String(20), comment='座位数')
    wheelbase: Mapped[Optional[str]] = mapped_column(String(5), comment='轴距(mm)')
    weight: Mapped[Optional[str]] = mapped_column(String(5), comment='整备质量(kg)')
    trunk_volume: Mapped[Optional[str]] = mapped_column(String(20), comment='行李厢容积(L)')
    intake: Mapped[Optional[str]] = mapped_column(String(30), comment='进气形式')
    cylinder_arrangement: Mapped[Optional[str]] = mapped_column(String(4), comment='气缸排列形式')
    cylinders: Mapped[Optional[str]] = mapped_column(String(4), comment='气缸数(个)')
    admission_gear: Mapped[Optional[str]] = mapped_column(String(10), comment='配气机构')
    max_ps: Mapped[Optional[str]] = mapped_column(String(5), comment='最大马力(Ps)')
    max_n_m: Mapped[Optional[str]] = mapped_column('max_n-m', String(5), comment='最大扭矩(N·m)')
    fuel: Mapped[Optional[str]] = mapped_column(String(10), comment='燃料形式')
    fuel_grade: Mapped[Optional[str]] = mapped_column(String(30), comment='燃油标号')
    fuel_supply_system: Mapped[Optional[str]] = mapped_column(String(20), comment='供油方式')
    environmental_standards_org: Mapped[Optional[str]] = mapped_column(String(20), comment='环保标准')
    drive_mode: Mapped[Optional[str]] = mapped_column(String(20), comment='驱动方式')
    front_suspension_type: Mapped[Optional[str]] = mapped_column(String(50), comment='前悬架类型')
    rear_suspension_type: Mapped[Optional[str]] = mapped_column(String(50), comment='后悬架类型')
    power_type: Mapped[Optional[str]] = mapped_column(String(30), comment='助力类型')
    car_boty_type: Mapped[Optional[str]] = mapped_column(String(30), comment='车体结构')
    front_brake_type: Mapped[Optional[str]] = mapped_column(String(30), comment='前制动器类型')
    rear_brake_type: Mapped[Optional[str]] = mapped_column(String(30), comment='后制动器类型')
    parking_brake_type: Mapped[Optional[str]] = mapped_column(String(30), comment='驻车制动类型')
    front_tire_specifications: Mapped[Optional[str]] = mapped_column(String(30), comment='前轮胎规格')
    rear_tire_specifications: Mapped[Optional[str]] = mapped_column(String(30), comment='后轮胎规格')
    master_air_bag: Mapped[Optional[str]] = mapped_column(String(4), comment='主驾驶座安全气囊')
    sub_air_bag: Mapped[Optional[str]] = mapped_column(String(4), comment='副驾驶座安全气囊')
    front_side_air_bag: Mapped[Optional[str]] = mapped_column(String(4), comment='前排侧气囊')
    rear_side_air_bag: Mapped[Optional[str]] = mapped_column(String(4), comment='后排侧气囊')
    front_head_air_bag: Mapped[Optional[str]] = mapped_column(String(4), comment='前排头部气囊(气帘)')
    rear_head_air_bag: Mapped[Optional[str]] = mapped_column(String(4), comment='后排头部气囊(气帘)')
    tire_pressure_monitoring: Mapped[Optional[str]] = mapped_column(String(4), comment='胎压监测装置')
    ISOFIX_child_seat_interfaces: Mapped[Optional[str]] = mapped_column(String(4), comment='ISOFIX儿童座椅接口')
    car_internal_lock: Mapped[Optional[str]] = mapped_column(String(4), comment='车内中控锁')
    keyless_start_system: Mapped[Optional[str]] = mapped_column(String(4), comment='无钥匙启动系统')
    abs: Mapped[Optional[str]] = mapped_column(String(4), comment='ABS防抱死')
    brake_assist: Mapped[Optional[str]] = mapped_column(String(4), comment='刹车辅助(EBA/BAS/BA等)')
    stability_control: Mapped[Optional[str]] = mapped_column(String(4), comment='车身稳定控制(ESC/ESP/DSC等)')
    variable_suspension: Mapped[Optional[str]] = mapped_column(String(30), comment='可变悬架')
    electric_roof: Mapped[Optional[str]] = mapped_column(String(4), comment='电动天窗')
    panoramic_roof: Mapped[Optional[str]] = mapped_column(String(4), comment='全景天窗')
    multifunction_steering_wheel: Mapped[Optional[str]] = mapped_column(String(4), comment='多功能方向盘')
    cruise: Mapped[Optional[str]] = mapped_column(String(4), comment='定速巡航')
    front_parking_radar: Mapped[Optional[str]] = mapped_column(String(4), comment='前驻车雷达')
    rear_parking_radar: Mapped[Optional[str]] = mapped_column(String(4), comment='后驻车雷达')
    reverse_video_image: Mapped[Optional[str]] = mapped_column(String(4), comment='倒车视频影像')
    seat_material: Mapped[Optional[str]] = mapped_column(String(20), comment='座椅材质')
    electric_seat_memory: Mapped[Optional[str]] = mapped_column(String(4), comment='电动座椅记忆')
    front_seat_heating: Mapped[Optional[str]] = mapped_column(String(4), comment='前排座椅加热')
    rear_seat_heating: Mapped[Optional[str]] = mapped_column(String(4), comment='后排座椅加热')
    front_seat_ventilation: Mapped[Optional[str]] = mapped_column(String(4), comment='前排座椅通风')
    rear_seat_ventilation: Mapped[Optional[str]] = mapped_column(String(4), comment='后排座椅通风')
    gps: Mapped[Optional[str]] = mapped_column(String(4), comment='GPS导航系统')
    low_beam_lamp: Mapped[Optional[str]] = mapped_column(String(30), comment='近光灯')
    daytime_running_light: Mapped[Optional[str]] = mapped_column(String(4), comment='日间行车灯')
    automatic_headlights: Mapped[Optional[str]] = mapped_column(String(4), comment='自动头灯')
    front_fog_lamp: Mapped[Optional[str]] = mapped_column(String(4), comment='前雾灯')
    front_electric_window: Mapped[Optional[str]] = mapped_column(String(4), comment='前电动车窗')
    rear_electric_window: Mapped[Optional[str]] = mapped_column(String(4), comment='后电动车窗')
    mirror_electric_adjustment: Mapped[Optional[str]] = mapped_column(String(4), comment='后视镜电动调节')
    mirror_heating: Mapped[Optional[str]] = mapped_column(String(4), comment='后视镜加热')
    air_conditioning_control_mode: Mapped[Optional[str]] = mapped_column(String(30), comment='空调控制方式')
    color_outside: Mapped[Optional[str]] = mapped_column(Text, comment='外观颜色')
    color_outside_code: Mapped[Optional[str]] = mapped_column(Text, comment='外观颜色码')
    hl_configs: Mapped[Optional[str]] = mapped_column(String(200), comment='亮点配置代号')
    hl_configc: Mapped[Optional[str]] = mapped_column(String(200), comment='亮点配置')
    environmental_standards: Mapped[Optional[str]] = mapped_column(String(20), comment='清洗后的排量标准')


class YckCarBasicConfigEv(Base):
    __tablename__ = 'yck_car_basic_config_ev'

    id: Mapped[int] = mapped_column(MEDIUMINT(6), primary_key=True, comment='车型id')
    ee_type: Mapped[str] = mapped_column(String(100), nullable=False, comment='电机类型')
    ee_total_power: Mapped[str] = mapped_column(String(100), nullable=False, comment='电动机总功率(kW)')
    ee_total_torque: Mapped[str] = mapped_column(String(100), nullable=False, comment='电动机总扭矩(N.m)')
    ee_front_max_kw: Mapped[str] = mapped_column(String(100), nullable=False, comment='前电动机最大功率(kW)')
    ee_front_max_Nm: Mapped[str] = mapped_column(String(100), nullable=False, comment='前电动机最大扭矩(N.m)')
    ee_after_max_kw: Mapped[str] = mapped_column(String(100), nullable=False, comment='后电动机最大功率(kW)')
    ee_after_max_Nm: Mapped[str] = mapped_column(String(100), nullable=False, comment='后电动机最大扭矩(N.m)')
    ee_system_integrated_kw: Mapped[str] = mapped_column(String(100), nullable=False, comment='系统综合功率(kW)')
    ee_system_integrated_Nm: Mapped[str] = mapped_column(String(100), nullable=False, comment='系统综合扭矩(N.m)')
    ee_driving_number: Mapped[str] = mapped_column(String(100), nullable=False, comment='驱动电机数')
    ee_layout: Mapped[str] = mapped_column(String(100), nullable=False, comment='电机布局')
    ee_battery_type: Mapped[str] = mapped_column(String(100), nullable=False, comment='电池类型')
    ee_pure_electric_range: Mapped[str] = mapped_column(String(100), nullable=False, comment='工信部纯电续航里程(km)')
    ee_battery_capacity: Mapped[str] = mapped_column(String(100), nullable=False, comment='电池能量(kWh)')
    ee_power_consumption: Mapped[str] = mapped_column(String(100), nullable=False, comment='百公里耗电量(kWh/100km)')
    ee_battery_quality_assurance: Mapped[str] = mapped_column(String(100), nullable=False, comment='电池组质保')
    ee_fast_charging_time: Mapped[str] = mapped_column(String(100), nullable=False, comment='快充时间(h)')
    ee_slow_charging_time: Mapped[str] = mapped_column(String(100), nullable=False, comment='慢充时间(h)')
    ee_fast_charge___: Mapped[str] = mapped_column('ee_fast_charge(%)', String(100), nullable=False, comment='快充电量(%)')


class YckCarBasicSeries(Base):
    __tablename__ = 'yck_car_basic_series'

    series_id: Mapped[int] = mapped_column(INTEGER(15), primary_key=True, comment='车系id')
    series_group_name: Mapped[str] = mapped_column(String(30), nullable=False, comment='车系分组')
    series_name: Mapped[str] = mapped_column(String(30), nullable=False, comment='车系')
    brandid: Mapped[int] = mapped_column(INTEGER(15), nullable=False)
    brand_name: Mapped[str] = mapped_column(String(30), nullable=False)
    car_level: Mapped[str] = mapped_column(String(20), nullable=False, comment='车级别')
    is_import: Mapped[str] = mapped_column(String(2), nullable=False, comment='是否进口(1进口)')
