def filter(event)
  tx_id = event.get('transaction_id')
  amount = event.get('amount').to_f
  tx_type = event.get('transaction_type')
  
  event.set('run_id', 'RUN-FINAL-2026')
  event.set('pipeline_version', 'v5.0')
  
  if tx_id.nil? || tx_id.empty? || amount < 0 || tx_type.nil?
    event.set('status', 'REJECT')
    event.set('error_class', 'DATA_QUALITY_ERROR')
  else
    event.set('status', 'VALID')
  end
  return [event]
end
