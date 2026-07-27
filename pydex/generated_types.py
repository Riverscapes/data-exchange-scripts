"""
Generated from riverscapes.schema.graphql using generate_python_classes_from_graphql_api.py
"""

from enum import StrEnum
from typing import TypedDict


class AttributionRoleEnum(StrEnum):
    ANALYST = 'ANALYST'
    CONTRIBUTOR = 'CONTRIBUTOR'
    CO_FUNDER = 'CO_FUNDER'
    DESIGNER = 'DESIGNER'
    FUNDER = 'FUNDER'
    OWNER = 'OWNER'
    QA_QC = 'QA_QC'
    SUPPORTER = 'SUPPORTER'


class DatasetContainerTypesEnum(StrEnum):
    CommonDatasets = 'CommonDatasets'
    Configuration = 'Configuration'
    Datasets = 'Datasets'
    Inputs = 'Inputs'
    Intermediates = 'Intermediates'
    Logs = 'Logs'
    Outputs = 'Outputs'
    Products = 'Products'


class DatasetTypeEnum(StrEnum):
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


class DateWithinEnum(StrEnum):
    ONE_DAY = 'ONE_DAY'
    ONE_MONTH = 'ONE_MONTH'
    ONE_WEEK = 'ONE_WEEK'
    SIX_MONTHS = 'SIX_MONTHS'


class EntitiesWithImagesEnum(StrEnum):
    COLLECTION = 'COLLECTION'
    ORGANIZATION = 'ORGANIZATION'
    PROJECT = 'PROJECT'
    PROJECT_TYPE = 'PROJECT_TYPE'
    SAVED_SEARCH = 'SAVED_SEARCH'
    USER = 'USER'


class EntityDeleteActionsEnum(StrEnum):
    DELETE = 'DELETE'
    DELETE_COMPLETE = 'DELETE_COMPLETE'
    MAKE_PUBLIC = 'MAKE_PUBLIC'
    REQUEST_TRANSFER = 'REQUEST_TRANSFER'


class ImageTypeEnum(StrEnum):
    AVATAR = 'AVATAR'
    HERO = 'HERO'
    LOGO = 'LOGO'


class JobStatusEnum(StrEnum):
    FAILED = 'FAILED'
    PROCESSING = 'PROCESSING'
    READY = 'READY'
    SUCCESS = 'SUCCESS'
    UNKNOWN = 'UNKNOWN'


class MetaDataExtEnum(StrEnum):
    DATASET = 'DATASET'
    PROJECT = 'PROJECT'
    WAREHOUSE = 'WAREHOUSE'


class MetaDataTypeEnum(StrEnum):
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


class NotificationActionsEnum(StrEnum):
    CREATED = 'CREATED'
    DELETED = 'DELETED'
    RENAMED = 'RENAMED'
    TRANSFERRED = 'TRANSFERRED'
    UPDATED = 'UPDATED'


class NotificationOperationEnum(StrEnum):
    DELETE = 'DELETE'
    MARK_READ = 'MARK_READ'
    MARK_UNREAD = 'MARK_UNREAD'


class NotificationTypesEnum(StrEnum):
    COLLECTION = 'COLLECTION'
    ORGANIZATION = 'ORGANIZATION'
    PROJECT = 'PROJECT'
    SAVED_SEARCH = 'SAVED_SEARCH'
    USER = 'USER'


class OrganizationInviteRoleEnum(StrEnum):
    ADMIN = 'ADMIN'
    CONTRIBUTOR = 'CONTRIBUTOR'
    VIEWER = 'VIEWER'


class OrganizationInviteStateEnum(StrEnum):
    ACCEPTED = 'ACCEPTED'
    EXPIRED = 'EXPIRED'
    INVITED = 'INVITED'
    REJECTED = 'REJECTED'
    REQUESTED = 'REQUESTED'


class OrganizationRoleEnum(StrEnum):
    ADMIN = 'ADMIN'
    CONTRIBUTOR = 'CONTRIBUTOR'
    NONE = 'NONE'
    OWNER = 'OWNER'
    VIEWER = 'VIEWER'


class OwnerInputTypesEnum(StrEnum):
    ORGANIZATION = 'ORGANIZATION'
    USER = 'USER'


class ProjectDeleteChoicesEnum(StrEnum):
    DELETE = 'DELETE'
    DELETE_COMPLETE = 'DELETE_COMPLETE'


class ProjectGroupVisibilityEnum(StrEnum):
    PUBLIC = 'PUBLIC'
    SECRET = 'SECRET'


class ProjectTreeLayerTypeEnum(StrEnum):
    FILE = 'FILE'
    LINE = 'LINE'
    POINT = 'POINT'
    POLYGON = 'POLYGON'
    RASTER = 'RASTER'
    REPORT = 'REPORT'
    TIN = 'TIN'


class ProjectTypeStateEnum(StrEnum):
    ACTIVE = 'ACTIVE'
    DELETED = 'DELETED'
    SUGGESTED = 'SUGGESTED'


class ProjectVisibilityEnum(StrEnum):
    PRIVATE = 'PRIVATE'
    PUBLIC = 'PUBLIC'
    SECRET = 'SECRET'


class QAQCStateEnum(StrEnum):
    FAILED = 'FAILED'
    PASSED = 'PASSED'
    PROVISIONAL = 'PROVISIONAL'


class RampTypeEnum(StrEnum):
    DISCRETE = 'DISCRETE'
    EXACT = 'EXACT'
    INTERPOLATED = 'INTERPOLATED'


class SearchSortEnum(StrEnum):
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


class SearchableTypesEnum(StrEnum):
    COLLECTION = 'COLLECTION'
    ORGANIZATION = 'ORGANIZATION'
    PROJECT = 'PROJECT'
    SAVED_SEARCH = 'SAVED_SEARCH'
    USER = 'USER'


class SeverityEnum(StrEnum):
    CRITICAL = 'CRITICAL'
    DEBUG = 'DEBUG'
    ERROR = 'ERROR'
    INFO = 'INFO'
    WARNING = 'WARNING'


class StarrableTypesEnum(StrEnum):
    COLLECTION = 'COLLECTION'
    ORGANIZATION = 'ORGANIZATION'
    PROJECT = 'PROJECT'
    SAVED_SEARCH = 'SAVED_SEARCH'
    USER = 'USER'


class SymbologyStateEnum(StrEnum):
    ERROR = 'ERROR'
    FETCHING = 'FETCHING'
    FOUND = 'FOUND'
    MISSING = 'MISSING'
    NOT_APPLICABLE = 'NOT_APPLICABLE'
    UNKNOWN = 'UNKNOWN'


class TileTypesEnum(StrEnum):
    HTML = 'HTML'
    RASTER = 'RASTER'
    VECTOR_GPKG = 'VECTOR_GPKG'
    VECTOR_SHP = 'VECTOR_SHP'


class TilingStateEnum(StrEnum):
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


class TransferStateEnum(StrEnum):
    ACCEPTED = 'ACCEPTED'
    EXPIRED = 'EXPIRED'
    IN_PROGRESS = 'IN_PROGRESS'
    PROPOSED = 'PROPOSED'
    REJECTED = 'REJECTED'


class TransferrableTypesEnum(StrEnum):
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
    createdOn: str
    id: str
    name: str
    summary: str
    updatedById: str
    updatedByName: str
    updatedOn: str


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
    size: int


class LinkInput(TypedDict, total=False):
    alt: str
    href: str
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
    preferences: dict
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
    preferences: dict
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
    totalSize: int
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
    url: str


class QAQCEventInput(TypedDict, total=False):
    datePerformed: str
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


SearchDateInput = TypedDict(
    'SearchDateInput',
    {
        'from': 'str',
        'to': 'str',
    },
    total=False,
)


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
    website: str


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
    url: str
