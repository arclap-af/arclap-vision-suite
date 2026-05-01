"""Authentication endpoints: login, logout, whoami, password change.

POST /api/auth/login            { username, password } -> sets cookie
POST /api/auth/logout           clears cookie
GET  /api/auth/whoami           -> current session user (or 401)
POST /api/auth/change-password  { old, new }
GET  /api/auth/users            list users (admin only)
POST /api/auth/users            create user (admin only)
POST /api/auth/users/{u}/disable
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

from core import auth as auth_core

router = APIRouter(tags=["auth"])


class LoginIn(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1)


class ChangePasswordIn(BaseModel):
    old_password: str
    new_password: str = Field(min_length=8)


class CreateUserIn(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=8)
    role: str = Field(default="viewer")


def _require_admin(request: Request):
    if auth_core.is_auth_disabled():
        return {"u": "dev", "r": "admin"}
    sess = auth_core.parse_session(request.cookies.get(auth_core.SESSION_COOKIE))
    if not sess or sess.get("r") != "admin":
        raise HTTPException(403, "Admin only")
    return sess


@router.post("/api/auth/login")
def login(req: LoginIn, response: Response):
    user = auth_core.verify_password(req.username, req.password)
    if not user:
        raise HTTPException(401, "Invalid credentials")
    cookie = auth_core.make_session(user["username"], user["role"])
    response.set_cookie(
        key=auth_core.SESSION_COOKIE,
        value=cookie,
        max_age=auth_core.SESSION_TTL,
        httponly=True,
        samesite="lax",
        secure=False,   # set True behind HTTPS reverse proxy
    )
    return {"ok": True, "username": user["username"], "role": user["role"]}


@router.post("/api/auth/logout")
def logout(response: Response):
    response.delete_cookie(auth_core.SESSION_COOKIE)
    return {"ok": True}


@router.get("/api/auth/whoami")
def whoami(request: Request):
    if auth_core.is_auth_disabled():
        return {"username": "dev", "role": "admin", "auth_disabled": True}
    sess = auth_core.parse_session(request.cookies.get(auth_core.SESSION_COOKIE))
    if not sess:
        raise HTTPException(401, "Not signed in")
    return {"username": sess["u"], "role": sess["r"]}


@router.post("/api/auth/change-password")
def change_password(req: ChangePasswordIn, request: Request):
    sess = auth_core.parse_session(request.cookies.get(auth_core.SESSION_COOKIE))
    if not sess and not auth_core.is_auth_disabled():
        raise HTTPException(401, "Not signed in")
    username = sess["u"] if sess else "admin"
    ok = auth_core.change_password(username, req.old_password, req.new_password)
    if not ok:
        raise HTTPException(400, "Old password wrong or new password rejected")
    return {"ok": True}


@router.get("/api/auth/users")
def list_users(request: Request):
    _require_admin(request)
    return {"users": auth_core.list_users()}


@router.post("/api/auth/users")
def create_user(req: CreateUserIn, request: Request):
    _require_admin(request)
    if req.role not in auth_core.ROLES:
        raise HTTPException(400, f"role must be one of {auth_core.ROLES}")
    try:
        auth_core.create_user(req.username, req.password, req.role)
    except Exception as e:
        raise HTTPException(400, f"Could not create user: {e}")
    return {"ok": True, "username": req.username, "role": req.role}


@router.post("/api/auth/users/{username}/disable")
def disable(username: str, request: Request):
    _require_admin(request)
    ok = auth_core.disable_user(username)
    if not ok:
        raise HTTPException(404, "User not found")
    return {"ok": True}
