"""
preprocessors/base.py
─────────────────────
SourceRegistry: مخزن سورس‌کد custom preprocessor‌ها.

هدف:
  هنگام ذخیره artifact، سورس هر کلاسی که اینجا ثبت شده
  داخل artifact embed می‌شود. هنگام بارگذاری، همان سورس
  در یک module جداگانه اجرا می‌شود تا Ray بتواند
  Preprocessor.deserialize() را بدون هیچ import خارجی انجام دهد.
"""

from __future__ import annotations
import types
import textwrap
import builtins
from pathlib import Path


class SourceRegistry:
    """
    یک registry ساده برای ثبت سورس‌کد custom preprocessor‌ها.

    استفاده:
        registry = SourceRegistry()
        registry.register(OutlierImputer)          # از روی __file__ می‌خواند
        registry.register_source("MyClass", src)   # مستقیم از رشته

        # هنگام save:
        blob = registry.export()   # dict[name -> source_str]

        # هنگام load:
        SourceRegistry.restore(blob)  # کلاس‌ها را در builtins inject می‌کند
    """

    def __init__(self) -> None:
        self._sources: dict[str, str] = {}

    # ─── ثبت ────────────────────────────────────────────────────────────────

    def register(self, cls: type) -> None:
        """سورس کل فایلی که cls در آن تعریف شده را ثبت می‌کند."""
        src_file = Path(cls.__module__.replace(".", "/") + ".py")
        # اگر فایل در همین پکیج باشد مسیر relative را امتحان می‌کند
        candidates = [
            src_file,
            Path(__file__).parent / (cls.__name__.lower() + ".py"),
        ]
        for path in candidates:
            if path.exists():
                self._sources[cls.__name__] = path.read_text(encoding="utf-8")
                return
        raise FileNotFoundError(
            f"سورس کلاس {cls.__name__!r} پیدا نشد. "
            "از register_source() با سورس رشته‌ای استفاده کنید."
        )

    def register_source(self, name: str, source: str) -> None:
        """ثبت مستقیم سورس به صورت رشته."""
        self._sources[name] = source

    # ─── export / restore ───────────────────────────────────────────────────

    def export(self) -> dict[str, str]:
        """برگرداندن کپی sources برای embed در artifact."""
        return dict(self._sources)

    @staticmethod
    def restore(sources: dict[str, str]) -> None:
        """
        اجرای سورس هر کلاس در یک module جداگانه
        و inject به builtins تا Ray.deserialize() آن را پیدا کند.
        """
        for name, src in sources.items():
            mod = types.ModuleType(f"_embedded_{name}")
            exec(textwrap.dedent(src), mod.__dict__)
            if name in mod.__dict__:
                setattr(builtins, name, mod.__dict__[name])
