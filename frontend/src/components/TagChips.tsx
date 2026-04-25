import { Link } from "react-router-dom";
import type { Tag } from "../api/client";

export default function TagChips({
  tags,
  excludeId,
}: {
  tags?: Tag[] | null;
  excludeId?: string;
}) {
  if (!tags || tags.length === 0) return null;
  const visible = excludeId ? tags.filter((t) => t.id !== excludeId) : tags;
  if (visible.length === 0) return null;
  return (
    <div className="tag-chips">
      {visible.map((t) => (
        <Link key={t.id} to={`/tags/${t.id}`} className="tag-chip">
          {t.name}
        </Link>
      ))}
    </div>
  );
}
