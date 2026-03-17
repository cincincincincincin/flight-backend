from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra='ignore',
    )

    # Database
    database_url: str = Field(..., description="PostgreSQL connection URL")

    # Redis
    redis_url: str = Field("redis://localhost:6379", description="Redis connection URL")

    # API keys
    aerodatabox_api_key: str = Field('', description="AeroDataBox / RapidAPI key")
    aviasales_api_token: str = Field('', description="Aviasales API token")
    rapidapi_host: str = Field('aerodatabox.p.rapidapi.com', description="RapidAPI host")

    # App
    app_name: str = "Flight Map API"
    debug: bool = False
    log_level: str = "INFO"

    # CORS
    cors_origins: List[str] = ["*"]

    # Auth - Supabase
    supabase_url: str = Field('', description="Supabase project URL for JWKS verification")
    supabase_jwt_secret: str = Field('', description="Supabase JWT secret for HS256 token verification")

    @field_validator('database_url')
    @classmethod
    def database_url_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError('DATABASE_URL must not be empty')
        return v

    @field_validator('log_level')
    @classmethod
    def log_level_valid(cls, v: str) -> str:
        valid = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
        upper = v.upper()
        if upper not in valid:
            raise ValueError(f'log_level must be one of {valid}')
        return upper


settings = Settings()  # type: ignore[call-arg]
