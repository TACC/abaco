from tapipy.tapis import Tapis

t = Tapis(base_url=base_url,
            username=username,
            account_type=account_type,
            tenant_id=tenant_id,
            password=password)
t.get_tokens()