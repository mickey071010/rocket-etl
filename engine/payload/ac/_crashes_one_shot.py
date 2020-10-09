import csv, json, requests, sys, traceback
from datetime import datetime
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack
from engine.parameters.remote_parameters import TEST_PACKAGE_ID

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa


class CrashSchema(pl.BaseSchema):
    crash_crn = fields.String(dump_to="CRASH_CRN", allow_none=False)
    district = fields.String(dump_to="DISTRICT", allow_none=True)
    crash_county = fields.String(dump_to="CRASH_COUNTY", allow_none=True)
    municipality = fields.String(dump_to="MUNICIPALITY", allow_none=True)
    police_agcy = fields.String(dump_to="POLICE_AGCY", allow_none=True)
    crash_year = fields.Integer(dump_to="CRASH_YEAR", allow_none=True)
    crash_month = fields.String(dump_to="CRASH_MONTH", allow_none=True)
    day_of_week = fields.Integer(dump_to="DAY_OF_WEEK", allow_none=True)
    time_of_day = fields.String(dump_to="TIME_OF_DAY", allow_none=True)
    hour_of_day = fields.String(dump_to="HOUR_OF_DAY", allow_none=True)
    illumination = fields.String(dump_to="ILLUMINATION", allow_none=True)
    weather = fields.String(dump_to="WEATHER", allow_none=True)
    road_condition = fields.String(dump_to="ROAD_CONDITION", allow_none=True)
    collision_type = fields.String(dump_to="COLLISION_TYPE", allow_none=True)
    relation_to_road = fields.String(dump_to="RELATION_TO_ROAD", allow_none=True)
    intersect_type = fields.String(dump_to="INTERSECT_TYPE", allow_none=True)
    tcd_type = fields.String(dump_to="TCD_TYPE", allow_none=True)
    urban_rural = fields.String(dump_to="URBAN_RURAL", allow_none=True)
    location_type = fields.String(dump_to="LOCATION_TYPE", allow_none=True)
    sch_bus_ind = fields.String(dump_to="SCH_BUS_IND", allow_none=True)
    sch_zone_ind = fields.String(dump_to="SCH_ZONE_IND", allow_none=True)
    total_units = fields.Integer(dump_to="TOTAL_UNITS", allow_none=True)
    person_count = fields.Integer(dump_to="PERSON_COUNT", allow_none=True)
    vehicle_count = fields.Integer(dump_to="VEHICLE_COUNT", allow_none=True)
    automobile_count = fields.Integer(dump_to="AUTOMOBILE_COUNT", allow_none=True)
    motorcycle_count = fields.Integer(dump_to="MOTORCYCLE_COUNT", allow_none=True)
    bus_count = fields.Integer(dump_to="BUS_COUNT", allow_none=True)
    small_truck_count = fields.Integer(dump_to="SMALL_TRUCK_COUNT", allow_none=True)
    heavy_truck_count = fields.Integer(dump_to="HEAVY_TRUCK_COUNT", allow_none=True)
    suv_count = fields.Integer(dump_to="SUV_COUNT", allow_none=True)
    van_count = fields.Integer(dump_to="VAN_COUNT", allow_none=True)
    bicycle_count = fields.Integer(dump_to="BICYCLE_COUNT", allow_none=True)
    fatal_count = fields.Integer(dump_to="FATAL_COUNT", allow_none=True)
    injury_count = fields.Integer(dump_to="INJURY_COUNT", allow_none=True)
    maj_inj_count = fields.Integer(dump_to="MAJ_INJ_COUNT", allow_none=True) # PennDOT is not supplying this field in
    # 2019, but they are offering SUSPECTED_SERIOUS, so the County is giving us those values as MAJ_INJ_COUNT.
    mod_inj_count = fields.Integer(dump_to="MOD_INJ_COUNT", allow_none=True) # The 2019 data provided by the County has
    # this field (for backward compatibility), but as PennDOT is not supplying these values, this column is entirely blank.
    min_inj_count = fields.Integer(dump_to="MIN_INJ_COUNT", allow_none=True)
    unk_inj_deg_count = fields.Integer(dump_to="UNK_INJ_DEG_COUNT", allow_none=True)
    unk_inj_per_count = fields.Integer(dump_to="UNK_INJ_PER_COUNT", allow_none=True)
    unbelted_occ_count = fields.Integer(dump_to="UNBELTED_OCC_COUNT", allow_none=True)
    unb_death_count = fields.Integer(dump_to="UNB_DEATH_COUNT", allow_none=True)
    unb_maj_inj_count = fields.Integer(dump_to="UNB_MAJ_INJ_COUNT", allow_none=True)
    belted_death_count = fields.Integer(dump_to="BELTED_DEATH_COUNT", allow_none=True)
    belted_maj_inj_count = fields.Integer(dump_to="BELTED_MAJ_INJ_COUNT", allow_none=True)
    mcycle_death_count = fields.Integer(dump_to="MCYCLE_DEATH_COUNT", allow_none=True)
    mcycle_maj_inj_count = fields.Integer(dump_to="MCYCLE_MAJ_INJ_COUNT", allow_none=True)
    bicycle_death_count = fields.Integer(dump_to="BICYCLE_DEATH_COUNT", allow_none=True)
    bicycle_maj_inj_count = fields.Integer(dump_to="BICYCLE_MAJ_INJ_COUNT", allow_none=True)
    ped_count = fields.Integer(dump_to="PED_COUNT", allow_none=True)
    ped_death_count = fields.Integer(dump_to="PED_DEATH_COUNT", allow_none=True)
    ped_maj_inj_count = fields.Integer(dump_to="PED_MAJ_INJ_COUNT", allow_none=True)
    comm_veh_count = fields.Integer(dump_to="COMM_VEH_COUNT", allow_none=True)
    max_severity_level = fields.Integer(dump_to="MAX_SEVERITY_LEVEL", allow_none=True)
    driver_count_16yr = fields.Integer(dump_to="DRIVER_COUNT_16YR", allow_none=True)
    driver_count_17yr = fields.Integer(dump_to="DRIVER_COUNT_17YR", allow_none=True)
    driver_count_18yr = fields.Integer(dump_to="DRIVER_COUNT_18YR", allow_none=True)
    driver_count_19yr = fields.Integer(dump_to="DRIVER_COUNT_19YR", allow_none=True)
    driver_count_20yr = fields.Integer(dump_to="DRIVER_COUNT_20YR", allow_none=True)
    driver_count_50_64yr = fields.Integer(dump_to="DRIVER_COUNT_50_64YR", allow_none=True)
    driver_count_65_74yr = fields.Integer(dump_to="DRIVER_COUNT_65_74YR", allow_none=True)
    driver_count_75plus = fields.Integer(dump_to="DRIVER_COUNT_75PLUS", allow_none=True)
    latitude = fields.String(dump_to="LATITUDE", allow_none=True)
    longitude = fields.String(dump_to="LONGITUDE", allow_none=True)
    dec_lat = fields.Float(dump_to="DEC_LAT", allow_none=True)
    dec_long = fields.Float(dump_to="DEC_LONG", allow_none=True)
    est_hrs_closed = fields.Integer(dump_to="EST_HRS_CLOSED", allow_none=True)
    lane_closed = fields.Integer(dump_to="LANE_CLOSED", allow_none=True)
    ln_close_dir = fields.String(dump_to="LN_CLOSE_DIR", allow_none=True)
    ntfy_hiwy_maint = fields.String(dump_to="NTFY_HIWY_MAINT", allow_none=True)
    rdwy_surf_type_cd = fields.String(dump_to="RDWY_SURF_TYPE_CD", allow_none=True)
    spec_juris_cd = fields.String(dump_to="SPEC_JURIS_CD", allow_none=True)
    tcd_func_cd = fields.String(dump_to="TCD_FUNC_CD", allow_none=True)
    tfc_detour_ind = fields.String(dump_to="TFC_DETOUR_IND", allow_none=True)
    work_zone_ind = fields.String(dump_to="WORK_ZONE_IND", allow_none=True)
    work_zone_type = fields.String(dump_to="WORK_ZONE_TYPE", allow_none=True)
    work_zone_loc = fields.String(dump_to="WORK_ZONE_LOC", allow_none=True)
    cons_zone_spd_lim = fields.Integer(dump_to="CONS_ZONE_SPD_LIM", allow_none=True)
    workers_pres = fields.String(dump_to="WORKERS_PRES", allow_none=True)
    wz_close_detour = fields.String(dump_to="WZ_CLOSE_DETOUR", allow_none=True)
    wz_flagger = fields.String(dump_to="WZ_FLAGGER", allow_none=True)
    wz_law_offcr_ind = fields.String(dump_to="WZ_LAW_OFFCR_IND", allow_none=True)
    wz_ln_closure = fields.String(dump_to="WZ_LN_CLOSURE", allow_none=True)
    wz_moving = fields.String(dump_to="WZ_MOVING", allow_none=True)
    wz_other = fields.String(dump_to="WZ_OTHER", allow_none=True)
    wz_shlder_mdn = fields.String(dump_to="WZ_SHLDER_MDN", allow_none=True)
    flag_crn = fields.String(dump_to="FLAG_CRN", allow_none=True)
    interstate = fields.Integer(dump_to="INTERSTATE", allow_none=True)
    state_road = fields.Integer(dump_to="STATE_ROAD", allow_none=True)
    local_road = fields.Integer(dump_to="LOCAL_ROAD", allow_none=True)
    local_road_only = fields.Integer(dump_to="LOCAL_ROAD_ONLY", allow_none=True)
    turnpike = fields.Integer(dump_to="TURNPIKE", allow_none=True)
    wet_road = fields.Integer(dump_to="WET_ROAD", allow_none=True)
    snow_slush_road = fields.Integer(dump_to="SNOW_SLUSH_ROAD", allow_none=True)
    icy_road = fields.Integer(dump_to="ICY_ROAD", allow_none=True)
    sudden_deer = fields.Integer(dump_to="SUDDEN_DEER", allow_none=True)
    shldr_related = fields.Integer(dump_to="SHLDR_RELATED", allow_none=True)
    rear_end = fields.Integer(dump_to="REAR_END", allow_none=True)
    ho_oppdir_sdswp = fields.Integer(dump_to="HO_OPPDIR_SDSWP", allow_none=True)
    hit_fixed_object = fields.Integer(dump_to="HIT_FIXED_OBJECT", allow_none=True)
    sv_run_off_rd = fields.Integer(dump_to="SV_RUN_OFF_RD", allow_none=True)
    work_zone = fields.Integer(dump_to="WORK_ZONE", allow_none=True)
    property_damage_only = fields.Integer(dump_to="PROPERTY_DAMAGE_ONLY", allow_none=True)
    fatal_or_maj_inj = fields.Integer(dump_to="FATAL_OR_MAJ_INJ", allow_none=True)
    injury = fields.Integer(dump_to="INJURY", allow_none=True)
    fatal = fields.Integer(dump_to="FATAL", allow_none=True)
    non_intersection = fields.Integer(dump_to="NON_INTERSECTION", allow_none=True)
    intersection = fields.Integer(dump_to="INTERSECTION", allow_none=True)
    signalized_int = fields.Integer(dump_to="SIGNALIZED_INT", allow_none=True)
    stop_controlled_int = fields.Integer(dump_to="STOP_CONTROLLED_INT", allow_none=True)
    unsignalized_int = fields.Integer(dump_to="UNSIGNALIZED_INT", allow_none=True)
    school_bus = fields.Integer(dump_to="SCHOOL_BUS", allow_none=True)
    school_zone = fields.Integer(dump_to="SCHOOL_ZONE", allow_none=True)
    hit_deer = fields.Integer(dump_to="HIT_DEER", allow_none=True)
    hit_tree_shrub = fields.Integer(dump_to="HIT_TREE_SHRUB", allow_none=True)
    hit_embankment = fields.Integer(dump_to="HIT_EMBANKMENT", allow_none=True)
    hit_pole = fields.Integer(dump_to="HIT_POLE", allow_none=True)
    hit_gdrail = fields.Integer(dump_to="HIT_GDRAIL", allow_none=True)
    hit_gdrail_end = fields.Integer(dump_to="HIT_GDRAIL_END", allow_none=True)
    hit_barrier = fields.Integer(dump_to="HIT_BARRIER", allow_none=True)
    hit_bridge = fields.Integer(dump_to="HIT_BRIDGE", allow_none=True)
    overturned = fields.Integer(dump_to="OVERTURNED", allow_none=True)
    motorcycle = fields.Integer(dump_to="MOTORCYCLE", allow_none=True)
    bicycle = fields.Integer(dump_to="BICYCLE", allow_none=True)
    hvy_truck_related = fields.Integer(dump_to="HVY_TRUCK_RELATED", allow_none=True)
    vehicle_failure = fields.Integer(dump_to="VEHICLE_FAILURE", allow_none=True)
    train_trolley = fields.Integer(dump_to="TRAIN_TROLLEY", allow_none=True)
    phantom_vehicle = fields.Integer(dump_to="PHANTOM_VEHICLE", allow_none=True)
    alcohol_related = fields.Integer(dump_to="ALCOHOL_RELATED", allow_none=True)
    drinking_driver = fields.Integer(dump_to="DRINKING_DRIVER", allow_none=True)
    underage_drnk_drv = fields.Integer(dump_to="UNDERAGE_DRNK_DRV", allow_none=True)
    unlicensed = fields.Integer(dump_to="UNLICENSED", allow_none=True)
    cell_phone = fields.Integer(dump_to="CELL_PHONE", allow_none=True)
    no_clearance = fields.Integer(dump_to="NO_CLEARANCE", allow_none=True)
    running_red_lt = fields.Integer(dump_to="RUNNING_RED_LT", allow_none=True)
    tailgating = fields.Integer(dump_to="TAILGATING", allow_none=True)
    cross_median = fields.Integer(dump_to="CROSS_MEDIAN", allow_none=True)
    curve_dvr_error = fields.Integer(dump_to="CURVE_DVR_ERROR", allow_none=True)
    limit_65mph = fields.Integer(dump_to="LIMIT_65MPH", allow_none=True)
    speeding = fields.Integer(dump_to="SPEEDING", allow_none=True)
    speeding_related = fields.Integer(dump_to="SPEEDING_RELATED", allow_none=True)
    aggressive_driving = fields.Integer(dump_to="AGGRESSIVE_DRIVING", allow_none=True)
    fatigue_asleep = fields.Integer(dump_to="FATIGUE_ASLEEP", allow_none=True)
    driver_16yr = fields.Integer(dump_to="DRIVER_16YR", allow_none=True)
    driver_17yr = fields.Integer(dump_to="DRIVER_17YR", allow_none=True)
    driver_65_74yr = fields.Integer(dump_to="DRIVER_65_74YR", allow_none=True)
    driver_75plus = fields.Integer(dump_to="DRIVER_75PLUS", allow_none=True)
    unbelted = fields.Integer(dump_to="UNBELTED", allow_none=True)
    pedestrian = fields.Integer(dump_to="PEDESTRIAN", allow_none=True)
    distracted = fields.Integer(dump_to="DISTRACTED", allow_none=True)
    curved_road = fields.Integer(dump_to="CURVED_ROAD", allow_none=True)
    driver_18yr = fields.Integer(dump_to="DRIVER_18YR", allow_none=True)
    driver_19yr = fields.Integer(dump_to="DRIVER_19YR", allow_none=True)
    driver_20yr = fields.Integer(dump_to="DRIVER_20YR", allow_none=True)
    driver_50_64yr = fields.Integer(dump_to="DRIVER_50_64YR", allow_none=True)
    vehicle_towed = fields.Integer(dump_to="VEHICLE_TOWED", allow_none=True)
    fire_in_vehicle = fields.Integer(dump_to="FIRE_IN_VEHICLE", allow_none=True)
    hit_parked_vehicle = fields.Integer(dump_to="HIT_PARKED_VEHICLE", allow_none=True)
    mc_drinking_driver = fields.Integer(dump_to="MC_DRINKING_DRIVER", allow_none=True)
    drugged_driver = fields.Integer(dump_to="DRUGGED_DRIVER", allow_none=True)
    injury_or_fatal = fields.Integer(dump_to="INJURY_OR_FATAL", allow_none=True)
    comm_vehicle = fields.Integer(dump_to="COMM_VEHICLE", allow_none=True)
    impaired_driver = fields.Integer(dump_to="IMPAIRED_DRIVER", allow_none=True)
    deer_related = fields.Integer(dump_to="DEER_RELATED", allow_none=True)
    drug_related = fields.Integer(dump_to="DRUG_RELATED", allow_none=True)
    hazardous_truck = fields.Integer(dump_to="HAZARDOUS_TRUCK", allow_none=True)
    illegal_drug_related = fields.Integer(dump_to="ILLEGAL_DRUG_RELATED", allow_none=True)
    illumination_dark = fields.Integer(dump_to="ILLUMINATION_DARK", allow_none=True)
    minor_injury = fields.Integer(dump_to="MINOR_INJURY", allow_none=True)
    moderate_injury = fields.Integer(dump_to="MODERATE_INJURY", allow_none=True)
    major_injury = fields.Integer(dump_to="MAJOR_INJURY", allow_none=True)
    nhtsa_agg_driving = fields.Integer(dump_to="NHTSA_AGG_DRIVING", allow_none=True)
    psp_reported = fields.Integer(dump_to="PSP_REPORTED", allow_none=True)
    running_stop_sign = fields.Integer(dump_to="RUNNING_STOP_SIGN", allow_none=True)
    train = fields.Integer(dump_to="TRAIN", allow_none=True)
    trolley = fields.Integer(dump_to="TROLLEY", allow_none=True)
    roadway_crn = fields.String(dump_to="ROADWAY_CRN", allow_none=True)
    rdwy_seq_num = fields.Integer(dump_to="RDWY_SEQ_NUM", allow_none=True)
    adj_rdwy_seq = fields.Integer(dump_to="ADJ_RDWY_SEQ", allow_none=True)
    access_ctrl = fields.String(dump_to="ACCESS_CTRL", allow_none=True)
    roadway_county = fields.String(dump_to="ROADWAY_COUNTY", allow_none=True)
    lane_count = fields.Integer(dump_to="LANE_COUNT", allow_none=True)
    rdwy_orient = fields.String(dump_to="RDWY_ORIENT", allow_none=True)
    road_owner = fields.String(dump_to="ROAD_OWNER", allow_none=True)
    route = fields.String(dump_to="ROUTE", allow_none=True)
    speed_limit = fields.Integer(dump_to="SPEED_LIMIT", allow_none=True)
    segment = fields.String(dump_to="SEGMENT", allow_none=True)
    offset = fields.Integer(dump_to="OFFSET", allow_none=True)
    street_name = fields.String(dump_to="STREET_NAME", allow_none=True)
    tot_inj_count = fields.Integer(dump_to="TOT_INJ_COUNT", allow_none=True)
    school_bus_unit = fields.String(dump_to="SCHOOL_BUS_UNIT", allow_none=True)

    @pre_load
    def fix_lane_count(self, data):
        if data['lane_count'] in ['', None]:
            data['lane_count'] = None
        else:
            data['lane_count'] = int(float(data['lane_count']))

    @pre_load
    def fix_types(self, data):
        # Fixing of types is necessary since the 2016 data got typed
        # differently.
        if data['est_hrs_closed'] is not None:
            data['est_hrs_closed'] = int(float(data['est_hrs_closed']))
        if data['cons_zone_spd_lim'] is not None:
            data['cons_zone_spd_lim'] = int(float(data['cons_zone_spd_lim']))

        unconverted_boolean_fields = ['interstate', 'state_road', 'local_road_only',
                'turnpike', 'wet_road', 'snow_slush_road', 'icy_road', 'sudden_deer',
                'shldr_related', 'rear_end', 'ho_oppdir_sdswp', 'hit_fixed_object',
                'sv_run_off_rd', 'work_zone', 'property_damage_only', 'fatal_or_maj_inj',
                'injury', 'fatal', 'non_intersection', 'intersection', 'signalized_int',
                'stop_controlled_int', 'unsignalized_int', 'school_bus', 'school_zone',
                'hit_deer', 'hit_tree_shrub', 'hit_embankment', 'hit_pole', 'hit_gdrail',
                'hit_gdrail_end', 'hit_barrier', 'hit_bridge', 'overturned', 'motorcycle',
                'bicycle', 'hvy_truck_related', 'vehicle_failure', 'train_trolley',
                'phantom_vehicle', 'alcohol_related', 'drinking_driver', 'underage_drnk_drv',
                'unlicensed', 'cell_phone', 'no_clearance', 'running_red_lt', 'tailgating',
                'cross_median', 'curve_dvr_error', 'limit_65mph', 'speeding',
                'speeding_related', 'aggressive_driving', 'fatigue_asleep', 'driver_17yr',
                'driver_65_74yr', 'driver_75plus', 'unbelted', 'pedestrian', 'distracted',
                'curved_road', 'driver_18yr', 'driver_19yr', 'driver_20yr', 'driver_50_64yr',
                'vehicle_towed', 'fire_in_vehicle', 'hit_parked_vehicle', 'mc_drinking_driver',
                'drugged_driver', 'injury_or_fatal', 'comm_vehicle', 'impaired_driver',
                'drug_related', 'hazardous_truck', 'illegal_drug_related',
                'illumination_dark', 'minor_injury', 'moderate_injury', 'major_injury',
                'nhtsa_agg_driving', 'psp_reported', 'running_stop_sign', 'train',
                'trolley', 'deer_related'] # This new format first appeared in the 2018 data.
        yes_no_to_0_1 = {'Yes': 1, 'No': 0,
                '1.0': 1, '0.0': 0}
        for field in unconverted_boolean_fields:
            if data[field] not in ['0', '1', '', None]:
                data[field] = yes_no_to_0_1[data[field]]

        # Backward compatibility code to handle some fields in 2019 data
        boolean_fields_to_convert_back_to_y_n = ['ntfy_hiwy_maint', 'tfc_detour_ind']
            # For backward compatibility with previous years and the cumulative table.
        zero_one_to_y_n = {'Yes': 'Y', 'No': 'N',
                '1.0': 'Y', '0.0': 'N',
                '1': 'Y', '0': 'N',
                'Y': 'Y', 'N': 'N',
                '': None, None: None,
                'U': 'U'} # Preserve the 'U' value since it is not clear
                # from the PennDOT data dictionary what it means and whether
                # it can just be translated to None (though it probably means
                # that it is Unknown whether traffic was detoured, when
                # this value appears in the tfc_detour_ind field).
        for field in boolean_fields_to_convert_back_to_y_n:
            data[field] = zero_one_to_y_n[data[field]]

        fields_foolishly_cast_by_excel = ['crash_year', 'district', 'day_of_week',
                'illumination', 'weather', 'road_condition', 'collision_type',
                'total_units', 'relation_to_road', 'tcd_type', 'urban_rural',
                'person_count', 'vehicle_count', 'automobile_count',
                'motorcycle_count', 'bus_count', 'small_truck_count',
                'heavy_truck_count', 'suv_count', 'van_count',
                'bicycle_count', 'fatal_count', 'injury_count',
                'maj_inj_count', 'mod_inj_count', 'min_inj_count',
                'unk_inj_deg_count', 'unk_inj_per_count',
                'unbelted_occ_count', 'unb_death_count',
                'unb_maj_inj_count', 'belted_death_count',
                'belted_maj_inj_count', 'mcycle_death_count',
                'mcycle_maj_inj_count', 'bicycle_death_count',
                'bicycle_maj_inj_count', 'ped_count',
                'ped_death_count', 'ped_maj_inj_count',
                'comm_veh_count', 'max_severity_level',
                'driver_count_16yr', 'driver_count_17yr',
                'driver_count_18yr', 'driver_count_19yr',
                'driver_count_20yr', 'driver_count_50_64yr',
                'driver_count_65_74yr', 'driver_count_75plus',
                'est_hrs_closed', 'lane_closed', 'ln_close_dir',
                'rdwy_surf_type_cd', 'spec_juris_cd', 'tcd_func_cd',
                'work_zone_type', 'work_zone_loc', 'cons_zone_spd_lim',
                'interstate', 'state_road', 'local_road',
                'local_road_only', 'turnpike', 'wet_road',
                'snow_slush_road', 'snow_slush_road', 'icy_road',
                'sudden_deer', 'shldr_related', 'rear_end',
                'ho_oppdir_sdswp', 'hit_fixed_object',
                'sv_run_off_rd', 'work_zone', 'property_damage_only',
                'fatal_or_maj_inj', 'injury', 'fatal', 'non_intersection',
                'intersection', 'signalized_int', 'stop_controlled_int',
                'unsignalized_int', 'school_bus', 'school_zone',
                'hit_deer', 'hit_tree_shrub', 'hit_embankment',
                'hit_pole', 'hit_gdrail', 'hit_gdrail_end',
                'hit_barrier', 'hit_bridge', 'overturned',
                'motorcycle', 'bicycle', 'hvy_truck_related',
                'vehicle_failure', 'train_trolley', 'phantom_vehicle',
                'alcohol_related', 'drinking_driver', 'underage_drnk_drv',
                'unlicensed', 'cell_phone', 'no_clearance',
                'running_red_lt', 'tailgating', 'cross_median',
                'curve_dvr_error', 'limit_65mph', 'speeding',
                'speeding_related', 'aggressive_driving',
                'fatigue_asleep', 'driver_16yr', 'driver_17yr',
                'driver_18yr', 'driver_65_74yr', 'driver_75plus',
                'unbelted', 'pedestrian', 'distracted', 'curved_road',
                'driver_19yr', 'driver_20yr', 'driver_50_64yr',
                'vehicle_towed', 'fire_in_vehicle', 'hit_parked_vehicle',
                'mc_drinking_driver', 'drugged_driver', 'injury_or_fatal',
                'comm_vehicle', 'impaired_driver', 'deer_related',
                'drug_related', 'hazardous_truck', 'illegal_drug_related',
                'illumination_dark', 'minor_injury', 'moderate_injury',
                'major_injury', 'nhtsa_agg_driving', 'psp_reported',
                'running_stop_sign', 'train', 'trolley', 'rdwy_seq_num',
                'adj_rdwy_seq', 'access_ctrl', 'lane_count', 'road_owner',
                'speed_limit', 'segment', 'offset', 'tot_inj_count']

        for field in fields_foolishly_cast_by_excel:
            if data[field] not in [None, '']:
                data[field] = str(int(float(data[field])))

        # 2018 data includes leading zeros in times (e.g., 013000 for
        # 1:30am) and other fields. In the cumulative resource, all of the
        # previous data had these leading zeros except for 2016+2017 data;
        # however, data for the individual years does not have these
        # in cases like the 2015 data (and possibly all previous years).

        # The processing below is a solution to standardize these records
        # despite having lost some of the raw data (probably eaten by
        # the old CKAN).
        length_by_field = {'crash_county': 2, 'police_agcy': 5,
                'crash_month': 2, 'time_of_day': 4,
                'hour_of_day': 2, 'municipality': 5,
                'intersect_type': 2, 'location_type': 2,
                'route': 4, 'segment': 4}
        for field in length_by_field.keys():
            if data[field] is not None:
                if len(data[field]) != length_by_field[field] and len(data[field]) != 0:
                    data[field] = data[field].zfill(length_by_field[field])

    class Meta:
        ordered = True

crashes_package_id = "3130f583-9499-472b-bb5a-f63a6ff6059a" # Production version of Crash data

current_year = datetime.now().year
last_year = current_year - 1

#old_year = 2019 # Uncomment this and the 'historical' job
# below to process old data.

job_dicts = [
    {
        'job_code': 'crashes_last_year',
        'source_type': 'sftp',
        'source_dir': '',
        'source_file': f'{last_year}_crashes.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': CrashSchema,
        #'primary_key_fields': [],
        'always_wipe_data': False,
        'upload_method': 'upsert',
        'primary_key_fields': ['CRASH_CRN'],
        #'destinations': ['file'], # These lines are just for testing
        #'destination_file': f'{last_year}_crashes.csv', # purposes.
        'package': crashes_package_id,
        'resource_name': f'{last_year} Crash Data'
    },
    {
        'job_code': 'cumulative_crashes',
        'source_type': 'sftp',
        'source_dir': '',
        'source_file': f'{last_year}_crashes.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': CrashSchema,
        #'primary_key_fields': [],
        'always_wipe_data': False,
        'upload_method': 'upsert',
        'primary_key_fields': ['CRASH_CRN'],
        #'destinations': ['file'], # These lines are just for testing
        #'destination_file': f'{last_year}_crashes.csv', # purposes.
        'package': crashes_package_id,
        'resource_name': f'Cumulative Crash Data (2004-2018)',
    },
]
