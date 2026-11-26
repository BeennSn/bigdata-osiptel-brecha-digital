#!/usr/bin/env python3
from probar_integracion import decisiones
import json
print('INDICADORES_OK',json.dumps(decisiones(),ensure_ascii=False),flush=True)
