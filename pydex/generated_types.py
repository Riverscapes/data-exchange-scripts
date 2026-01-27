"""
Generated from riverscapes.schema.graphql using generate_python_classes_from_graphql_api.py
"""
from typing import TypedDict
from enum import Enum


class AttributionRoleEnum(str, Enum):
    ANALYST = 'ANALYST'
    CONTRIBUTOR = 'CONTRIBUTOR'
    CO_FUNDER = 'CO_FUNDER'
    DESIGNER = 'DESIGNER'
    FUNDER = 'FUNDER'
    OWNER = 'OWNER'
    QA_QC = 'QA_QC'
    SUPPORTER = 'SUPPORTER'


class DatasetContainerTypesEnum(str, Enum):
    CommonDatasets = 'CommonDatasets'
    Configuration = 'Configuration'
    Datasets = 'Datasets'
    Inputs = 'Inputs'
    Intermediates = 'Intermediates'
    Logs = 'Logs'
    Outputs = 'Outputs'
    Products = 'Products'


class DatasetTypeEnum(str, Enum):
    AuxInstrumentFile = 'AuxInstrumentFile'
    CSV = 'CSV'
    ConfigFile = 'ConfigFile'
    DEM = 'DEM'
    DataTable = 'DataTable'
    Database = 'Database'
    File = 'File'
    Geopackage = 'Geopackage'
    HTMLFile = 'HTMLFile'
    HillShade = 'HillShade'
    Image = 'Image'
    InstrumentFile = 'InstrumentFile'
    LogFile = 'LogFile'
    MSAccessDB = 'MSAccessDB'
    PDF = 'PDF'
    Raster = 'Raster'
    SQLiteDB = 'SQLiteDB'
    SurveyQualityDB = 'SurveyQualityDB'
    TIN = 'TIN'
    Vector = 'Vector'
    Video = 'Video'
    ZipFile = 'ZipFile'


class DateWithinEnum(str, Enum):
    ONE_DAY = 'ONE_DAY'
    ONE_MONTH = 'ONE_MONTH'
    ONE_WEEK = 'ONE_WEEK'
    SIX_MONTHS = 'SIX_MONTHS'


class EntitiesWithImagesEnum(str, Enum):
    COLLECTION = 'COLLECTION'
    ORGANIZATION = 'ORGANIZATION'
    PROJECT = 'PROJECT'
    PROJECT_TYPE = 'PROJECT_TYPE'
    SAVED_SEARCH = 'SAVED_SEARCH'
    USER = 'USER'


class EntityDeleteActionsEnum(str, Enum):
    DELETE = 'DELETE'
    DELETE_COMPLETE = 'DELETE_COMPLETE'
    MAKE_PUBLIC = 'MAKE_PUBLIC'
    REQUEST_TRANSFER = 'REQUEST_TRANSFER'


class ImageTypeEnum(str, Enum):
    AVATAR = 'AVATAR'
    HERO = 'HERO'
    LOGO = 'LOGO'


class JobStatusEnum(str, Enum):
    FAILED = 'FAILED'
    PROCESSING = 'PROCESSING'
    READY = 'READY'
    SUCCESS = 'SUCCESS'
    UNKNOWN = 'UNKNOWN'


class MetaDataExtEnum(str, Enum):
    DATASET = 'DATASET'
    PROJECT = 'PROJECT'
    WAREHOUSE = 'WAREHOUSE'


class MetaDataTypeEnum(str, Enum):
    BOOLEAN = 'BOOLEAN'
    FILEPATH = 'FILEPATH'
    FLOAT = 'FLOAT'
    GUID = 'GUID'
    HIDDEN = 'HIDDEN'
    IMAGE = 'IMAGE'
    INT = 'INT'
    ISODATE = 'ISODATE'
    JSON = 'JSON'
    MARKDOWN = 'MARKDOWN'
    RICHTEXT = 'RICHTEXT'
    STRING = 'STRING'
    TIMESTAMP = 'TIMESTAMP'
    URL = 'URL'
    VIDEO = 'VIDEO'


class NotificationActionsEnum(str, Enum):
    CREATED = 'CREATED'
    DELETED = 'DELETED'
    RENAMED = 'RENAMED'
    TRANSFERRED = 'TRANSFERRED'
    UPDATED = 'UPDATED'


class NotificationOperationEnum(str, Enum):
    DELETE = 'DELETE'
    MARK_READ = 'MARK_READ'
    MARK_UNREAD = 'MARK_UNREAD'


class NotificationTypesEnum(str, Enum):
    COLLECTION = 'COLLECTION'
    ORGANIZATION = 'ORGANIZATION'
    PROJECT = 'PROJECT'
    SAVED_SEARCH = 'SAVED_SEARCH'
    USER = 'USER'


class OrganizationInviteRoleEnum(str, Enum):
    ADMIN = 'ADMIN'
    CONTRIBUTOR = 'CONTRIBUTOR'
    VIEWER = 'VIEWER'


class OrganizationInviteStateEnum(str, Enum):
    ACCEPTED = 'ACCEPTED'
    EXPIRED = 'EXPIRED'
    INVITED = 'INVITED'
    REJECTED = 'REJECTED'
    REQUESTED = 'REQUESTED'


class OrganizationRoleEnum(str, Enum):
    ADMIN = 'ADMIN'
    CONTRIBUTOR = 'CONTRIBUTOR'
    NONE = 'NONE'
    OWNER = 'OWNER'
    VIEWER = 'VIEWER'


class OwnerInputTypesEnum(str, Enum):
    ORGANIZATION = 'ORGANIZATION'
    USER = 'USER'


class ProjectDeleteChoicesEnum(str, Enum):
    DELETE = 'DELETE'
    DELETE_COMPLETE = 'DELETE_COMPLETE'


class ProjectGroupVisibilityEnum(str, Enum):
    PUBLIC = 'PUBLIC'
    SECRET = 'SECRET'


class ProjectTreeLayerTypeEnum(str, Enum):
    FILE = 'FILE'
    LINE = 'LINE'
    POINT = 'POINT'
    POLYGON = 'POLYGON'
    RASTER = 'RASTER'
    REPORT = 'REPORT'
    TIN = 'TIN'


class ProjectTypeStateEnum(str, Enum):
    ACTIVE = 'ACTIVE'
    DELETED = 'DELETED'
    SUGGESTED = 'SUGGESTED'


class ProjectVisibilityEnum(str, Enum):
    PRIVATE = 'PRIVATE'
    PUBLIC = 'PUBLIC'
    SECRET = 'SECRET'


class QAQCStateEnum(str, Enum):
    FAILED = 'FAILED'
    PASSED = 'PASSED'
    PROVISIONAL = 'PROVISIONAL'


class RampTypeEnum(str, Enum):
    DISCRETE = 'DISCRETE'
    EXACT = 'EXACT'
    INTERPOLATED = 'INTERPOLATED'


class SearchSortEnum(str, Enum):
    AREA_DESC = 'AREA_DESC'
    DATE_CREATED_ASC = 'DATE_CREATED_ASC'
    DATE_CREATED_DESC = 'DATE_CREATED_DESC'
    DATE_UPDATED_ASC = 'DATE_UPDATED_ASC'
    DATE_UPDATED_DESC = 'DATE_UPDATED_DESC'
    MINE = 'MINE'
    MODEL_VERSION_ASC = 'MODEL_VERSION_ASC'
    MODEL_VERSION_DESC = 'MODEL_VERSION_DESC'
    NAME_ASC = 'NAME_ASC'
    NAME_DESC = 'NAME_DESC'


class SearchableTypesEnum(str, Enum):
    COLLECTION = 'COLLECTION'
    ORGANIZATION = 'ORGANIZATION'
    PROJECT = 'PROJECT'
    SAVED_SEARCH = 'SAVED_SEARCH'
    USER = 'USER'


class SeverityEnum(str, Enum):
    CRITICAL = 'CRITICAL'
    DEBUG = 'DEBUG'
    ERROR = 'ERROR'
    INFO = 'INFO'
    WARNING = 'WARNING'


class StarrableTypesEnum(str, Enum):
    COLLECTION = 'COLLECTION'
    ORGANIZATION = 'ORGANIZATION'
    PROJECT = 'PROJECT'
    SAVED_SEARCH = 'SAVED_SEARCH'
    USER = 'USER'


class SymbologyStateEnum(str, Enum):
    ERROR = 'ERROR'
    FETCHING = 'FETCHING'
    FOUND = 'FOUND'
    MISSING = 'MISSING'
    NOT_APPLICABLE = 'NOT_APPLICABLE'
    UNKNOWN = 'UNKNOWN'


class TileTypesEnum(str, Enum):
    HTML = 'HTML'
    RASTER = 'RASTER'
    VECTOR_GPKG = 'VECTOR_GPKG'
    VECTOR_SHP = 'VECTOR_SHP'


class TilingStateEnum(str, Enum):
    CREATING = 'CREATING'
    FETCHING = 'FETCHING'
    FETCH_ERROR = 'FETCH_ERROR'
    INDEX_NOT_FOUND = 'INDEX_NOT_FOUND'
    LAYER_NOT_FOUND = 'LAYER_NOT_FOUND'
    NOT_APPLICABLE = 'NOT_APPLICABLE'
    NO_GEOMETRIES = 'NO_GEOMETRIES'
    QUEUED = 'QUEUED'
    SUCCESS = 'SUCCESS'
    TILING_ERROR = 'TILING_ERROR'
    TIMEOUT = 'TIMEOUT'
    UNKNOWN = 'UNKNOWN'


class TransferStateEnum(str, Enum):
    ACCEPTED = 'ACCEPTED'
    EXPIRED = 'EXPIRED'
    IN_PROGRESS = 'IN_PROGRESS'
    PROPOSED = 'PROPOSED'
    REJECTED = 'REJECTED'


class TransferrableTypesEnum(str, Enum):
    COLLECTION = 'COLLECTION'
    ORGANIZATION = 'ORGANIZATION'
    PROJECT = 'PROJECT'
    USER = 'USER'


class CollectionInput(TypedDict, total=False):
    citation: str
    clearContact: bool
    clearHeroImage: bool
    contact: 'OwnerInput'
    description: str
    heroImageToken: str
    meta: list['MetaDataInput']
    name: str
    summary: str
    tags: list[str]
    visibility: 'ProjectGroupVisibilityEnum'


class DBObjNotificationsInput(TypedDict, total=False):
    createdById: str
    createdByName: str
    createdOn: 'DateTime'
    id: str
    name: str
    summary: str
    updatedById: str
    updatedByName: str
    updatedOn: 'DateTime'


class DatasetInput(TypedDict, total=False):
    citation: str
    description: str
    extRef: str
    layers: list['DatasetLayerInput']
    localPath: str
    meta: list['MetaDataInput']
    name: str
    rsXPath: str
    summary: str


class DatasetLayerInput(TypedDict, total=False):
    citation: str
    description: str
    extRef: str
    lyrName: str
    meta: list['MetaDataInput']
    name: str
    summary: str


class DatasetLayerUpdate(TypedDict, total=False):
    citation: str
    description: str
    meta: list['MetaDataInput']
    name: str
    summary: str


class DatasetUpdate(TypedDict, total=False):
    citation: str
    description: str
    dsId: str
    meta: list['MetaDataInput']
    name: str
    summary: str


class EntityDeletionOptions(TypedDict, total=False):
    totalDelete: bool
    transfer: 'TransferEntityItemsInput'


class FileDownloadMetaInput(TypedDict, total=False):
    contentType: str
    localPath: str
    md5: str
    size: 'BigInt'


class LinkInput(TypedDict, total=False):
    alt: str
    href: 'URL'
    text: str


class MetaDataInput(TypedDict, total=False):
    ext: 'MetaDataExtEnum'
    key: str
    locked: bool
    type: 'MetaDataTypeEnum'
    value: str


class NotificationInput(TypedDict, total=False):
    object: 'DBObjNotificationsInput'
    subject: 'DBObjNotificationsInput'
    type: 'NotificationTypesEnum'
    verb: 'NotificationActionsEnum'


class OrganizationInput(TypedDict, total=False):
    clearLogo: bool
    description: str
    logoToken: str
    meta: list['MetaDataInput']
    name: str
    preferences: 'JSONObject'
    social: 'SocialLinksInput'
    summary: str


class OwnerInput(TypedDict, total=False):
    id: str
    type: 'OwnerInputTypesEnum'


class ProfileInput(TypedDict, total=False):
    affiliations: list['UserAffiliationInput']
    avatarToken: str
    clearAvatar: bool
    description: str
    jobTitle: str
    location: str
    meta: list['MetaDataInput']
    name: str
    preferences: 'JSONObject'
    socialLinks: 'SocialLinksInput'
    summary: str


class ProjectAttributionInput(TypedDict, total=False):
    organizationId: str
    roles: list['AttributionRoleEnum']


class ProjectInput(TypedDict, total=False):
    archived: bool
    attribution: list['ProjectAttributionInput']
    boundsToken: str
    citation: str
    clearBounds: bool
    clearHeroImage: bool
    datasets: list['DatasetInput']
    deleteDatasets: list[str]
    description: str
    heroImageToken: str
    meta: list['MetaDataInput']
    name: str
    qaqc: list['QAQCEventInput']
    summary: str
    tags: list[str]
    totalSize: 'BigInt'
    visibility: 'ProjectVisibilityEnum'


class ProjectSearchParamsInput(TypedDict, total=False):
    attributedOrgId: str
    bbox: list[float]
    boundsId: str
    collection: str
    createdOn: 'SearchDateInput'
    createdWithin: 'DateWithinEnum'
    editableOnly: bool
    excludeArchived: bool
    keywords: str
    meta: list['MetaDataInput']
    name: str
    ownedBy: 'OwnerInput'
    projectTypeId: str
    tags: list[str]
    updatedOn: 'SearchDateInput'
    visibility: 'ProjectVisibilityEnum'


class ProjectTypeInput(TypedDict, total=False):
    clearLogo: bool
    description: str
    logoToken: str
    meta: list['MetaDataInput']
    name: str
    summary: str
    url: 'URL'


class QAQCEventInput(TypedDict, total=False):
    datePerformed: 'DateTime'
    description: str
    meta: list['MetaDataInput']
    name: str
    performedBy: str
    state: 'QAQCStateEnum'
    summary: str
    supportingLinks: list['LinkInput']


class SavedSearchInput(TypedDict, total=False):
    citation: str
    clearHeroImage: bool
    defaultSort: list['SearchSortEnum']
    description: str
    heroImageToken: str
    meta: list['MetaDataInput']
    name: str
    searchParams: 'ProjectSearchParamsInput'
    summary: str
    tags: list[str]
    visibility: 'ProjectGroupVisibilityEnum'


class SearchDateInput(TypedDict, total=False):
    from: 'DateTime'
    to: 'DateTime'


class SearchParamsInput(TypedDict, total=False):
    createdOn: 'SearchDateInput'
    createdWithin: 'DateWithinEnum'
    editableOnly: bool
    keywords: str
    meta: list['MetaDataInput']
    name: str
    ownedBy: 'OwnerInput'
    tags: list[str]
    updatedOn: 'SearchDateInput'
    visibility: 'ProjectGroupVisibilityEnum'


class SocialLinksInput(TypedDict, total=False):
    facebook: str
    instagram: str
    linkedIn: str
    tiktok: str
    twitter: str
    website: 'URL'


class TransferEntityItemsInput(TypedDict, total=False):
    note: str
    transferTo: 'OwnerInput'


class TransferInput(TypedDict, total=False):
    includeProjects: bool
    note: str
    objectIds: list[str]
    transferTo: 'OwnerInput'
    transferType: 'TransferrableTypesEnum'


class UserAffiliationInput(TypedDict, total=False):
    affiliationRole: str
    name: str
    url: 'URL'
