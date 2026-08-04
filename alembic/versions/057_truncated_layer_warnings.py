"""Say so on the 59 layers published short of their source

Audited every tracked GovMap layer on 4.8.2026 — published row count against the
count `filter/count` declares. 59 layers are short, by 1,956,643 records in
total, and not one of them says so. The reader downloads a file that looks
whole.

Four are missing more than a third of the layer:

    נחלים                 1,000,009 / 1,549,529   (65% present)
    נקודות בקרה             543,086 /   986,377   (55%)
    מעג"ל כבישים            385,742 /   748,570   (52%)
    יעודי קרקע - מבא''ת     550,299 /   788,513   (70%)

Two causes behind it. Five layers stop at GOVMAP_MAX_FEATURES (1,000,000) —
a deliberate cap the scraper's completeness gate excuses in as many words
("stopped at max_features … not a completeness failure") and then drops, so the
fact dies in a log line. The rest fall short below the cap, which points at the
box-walk depth limit or at the refusal-window bug govscraper fixed in 6ec9985,
where a refused quadrant was silently skipped and the walk reported success.

This is a SNAPSHOT, not a mechanism. OVER cannot detect truncation on its own:
scrape_metadata carries `total_items` and no declared count, so there is nothing
to compare against, and push_version clears the flag on the next successful
push whether or not the layer is still short. The durable fix belongs in the
scraper — emit `quality_warning` when it truncates, the channel push_version
already reads. Until then this at least stops presenting a 65%% layer as whole.

Revision ID: 057
Revises: 056
Create Date: 2026-08-04
"""
from typing import Sequence, Union

from alembic import op


revision: str = "057"
down_revision: Union[str, None] = "056"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE tracked_datasets t
        SET import_warning =
                'נמדד ב-4.8.2026: הגרסה האחרונה מכילה '
                || to_char(v.pub_n, 'FM999,999,999')
                || ' רשומות, בעוד המקור מצהיר על '
                || to_char(v.dec_n, 'FM999,999,999')
                || ' — כ-' || v.pct_n
                || '% מהשכבה אינם כלולים בגרסה זו. הרשומות שנשמרו תקינות, אך '
                || 'השכבה חלקית ואינה מייצגת את המקור במלואו.',
            import_warning_at = now()
        FROM (VALUES
    ('560a9c78-a3f6-4eef-93fb-db1bec71596b'::uuid, 1000009, 1549529, 35),
    ('950f1975-007d-469e-82e4-30290fd87cac'::uuid, 543086, 986377, 45),
    ('1812c679-942c-4428-98b3-56e983eab5bc'::uuid, 385742, 748570, 48),
    ('c077a2d5-8347-4509-96b4-b9c746459969'::uuid, 550299, 788513, 30),
    ('9b125307-2a3a-47a7-9ea8-85d5f2439810'::uuid, 1000000, 1097715, 9),
    ('68b8e2fe-d757-4478-aa46-ee756001f6bc'::uuid, 1000000, 1094327, 9),
    ('7b613f82-0f57-442b-abaf-29c5a8c504fa'::uuid, 1000000, 1092903, 9),
    ('126a3c3d-8ff0-469a-aea2-2b128675d7bf'::uuid, 1000008, 1054486, 5),
    ('232ec1ff-a061-4f24-9419-332d6d1053d5'::uuid, 228118, 242681, 6),
    ('d3e557c0-d4f6-4a37-8245-9ee3a8528c05'::uuid, 37228, 42381, 12),
    ('cf4cecc5-07a9-4be9-83e5-22a5d59e610f'::uuid, 2224, 3143, 29),
    ('b5756e0a-21fb-49ce-9215-adbe7211005b'::uuid, 2520, 3209, 21),
    ('e6a8ce08-c77f-44e7-9037-7f5022b74fb2'::uuid, 6646, 7173, 7),
    ('6aa1b60e-b9a9-4571-9213-81a1f5e7954e'::uuid, 128128, 128502, 1),
    ('4174aac2-c8b2-443e-9425-3773ce47c8da'::uuid, 313696, 313983, 1),
    ('9bf332ee-8a77-4111-8d6b-ef7bb92f6488'::uuid, 31849, 32027, 1),
    ('fb79e65e-0d1b-4beb-8d2f-a4490f7cb858'::uuid, 23544, 23675, 1),
    ('4d12d4d1-f9c8-4597-8a22-9cd1bd8adbba'::uuid, 24922, 25051, 1),
    ('21eb1598-60fa-41b6-b124-b179accc85c9'::uuid, 0, 82, 100),
    ('eecbe0cc-1a69-4600-9b42-bfd7c57730f0'::uuid, 402, 462, 13),
    ('1f4080dc-1f3f-4c21-8c22-df85590b2b40'::uuid, 1470, 1526, 4),
    ('b105ae22-a6f5-4cd0-b37e-c63dc5da40c3'::uuid, 36791, 36840, 1),
    ('2156f30e-cb97-4737-a2f5-bd030b61c944'::uuid, 37828, 37850, 1),
    ('ce456a39-808c-487e-9a79-368ed60f29af'::uuid, 580, 599, 3),
    ('ccac8d05-0474-49ab-baac-09846079aa12'::uuid, 24352, 24369, 1),
    ('5732e4a1-fd09-4048-aafc-958c68e81ec0'::uuid, 3029, 3044, 1),
    ('0c90b9ec-cf2c-4af3-b0ad-4894067d4598'::uuid, 10513, 10527, 1),
    ('be10c1ab-2396-4ae9-9afb-63dce3071040'::uuid, 319675, 319688, 1),
    ('73c27fe1-d14d-4374-8789-94667b9fc1cd'::uuid, 4371, 4383, 1),
    ('a32369d5-8313-4b1a-98cc-84ba05185bce'::uuid, 89131, 89142, 1),
    ('41734d96-9e22-4730-8d51-eb673c9baadb'::uuid, 35245, 35252, 1),
    ('5c838899-2294-4b1e-b9ed-ce0f28a3a29f'::uuid, 11, 16, 31),
    ('ba0874f2-5e49-41ac-b800-34cefd9715cf'::uuid, 7356, 7359, 1),
    ('b5ec3689-c49d-4114-b10f-a0c75d251803'::uuid, 196, 199, 2),
    ('a9e23fed-74b8-4e57-936b-1fee806328ff'::uuid, 33338, 33340, 1),
    ('2a1de5b9-79ea-4648-a427-70bcd439df03'::uuid, 116, 118, 2),
    ('721b73de-91b8-4dc6-873b-935c83d69fc7'::uuid, 86, 88, 2),
    ('d760ff9c-547b-4305-947d-2c4e619995be'::uuid, 573, 575, 1),
    ('98b2cde9-f638-4f82-85e8-9ac5710a6b4e'::uuid, 146, 147, 1),
    ('afd594e1-300f-4f5b-a85c-4c28c43eec9d'::uuid, 143, 144, 1),
    ('2879c28b-1f9c-4f2c-b516-f839e60caaf1'::uuid, 365, 366, 1),
    ('4c9d5517-63cb-445c-8511-48a398235208'::uuid, 18982, 18983, 1),
    ('14f189ef-59e8-4cef-b61d-36677a4bbd1c'::uuid, 16907, 16908, 1),
    ('f46efd5d-e65e-459c-bb6a-f1b512284f04'::uuid, 706, 707, 1),
    ('72af5ff2-e761-45dc-b9ef-bbc1fe10e8ba'::uuid, 8283, 8284, 1),
    ('2c6b3fe1-28d5-45e8-b99a-7fa5520498fb'::uuid, 197, 198, 1),
    ('ba1ddade-3b9e-484b-9b26-458b93d94d2d'::uuid, 430, 431, 1),
    ('93a05df1-2424-408a-8fe7-5efe96377150'::uuid, 363, 364, 1),
    ('3a0dd481-ce6e-4988-b706-04cb57597597'::uuid, 61, 62, 2),
    ('df3ee3f5-1985-4e8d-815e-0236bb1563b3'::uuid, 1082, 1083, 1),
    ('8cb0dbd7-3733-444e-95e5-e5983c118c01'::uuid, 35, 36, 3),
    ('53cb1ec6-4b1e-4eaf-8eb5-0926912515f1'::uuid, 3020, 3021, 1),
    ('0518439a-ec76-4ddb-8fe8-375e57b1d1d3'::uuid, 21, 22, 5),
    ('932d519d-bf11-4d1a-ba92-a7ad063a58f9'::uuid, 11, 12, 8),
    ('33473f81-1645-4d91-99b3-e7dbe48e513f'::uuid, 12, 13, 8),
    ('1264a87f-2b1d-4fee-9bc8-04c3b75e3adb'::uuid, 4135, 4136, 1),
    ('7f9efc9c-559e-4152-bfe0-f394a19912f0'::uuid, 104, 105, 1),
    ('2ea30aae-2bfc-4435-aa54-5270980bbb13'::uuid, 18601, 18602, 1),
    ('c2bd90e1-fb36-437a-878a-d01f1f4aea62'::uuid, 138, 139, 1)
        ) AS v(id, pub_n, dec_n, pct_n)
        WHERE t.id = v.id
        """
    )


def downgrade() -> None:
    op.execute(
        "UPDATE tracked_datasets SET import_warning = NULL, import_warning_at = NULL "
        "WHERE import_warning LIKE 'נמדד ב-4.8.2026%'"
    )
