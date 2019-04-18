local function split(str, sep)
	local result = {}
	local regex = ("([^%s]+)"):format(sep)
	for each in str:gmatch(regex) do
		table.insert(result, each)
	end

	return result
end

function hyc_merge_rec(rec, input_ids)
	trace("HYC_UDF args:: (%s)--------------------", input_ids);
	trace("HYC_UDF key:: (%s)--------------------", record.key(rec));

	local rec_vmdkid = -1; rec_ckptid = -1; rec_offset = -1
	local count = 0
	local vals = split(record.key(rec), ":")
	for _,val in ipairs(vals) do
		if count == 0 then
			rec_vmdkid = tonumber(val)
		elseif count == 1 then
			rec_ckptid = tonumber(val)
		elseif count == 2 then
			rec_offset = tonumber(val)
		end

		count = count + 1
	end

	trace("HYC_UDF Post split count : %d, vmdkid : %d, ckpt_id : %d, offset :%d", count, rec_vmdkid, rec_ckptid, rec_offset);
	if vmdkid == -1 or ckpt_id == -1 or offset == -1 or count ~= 3 then
		trace("HYC_UDF Error, Invalid record format");
	else
		local elem
		local result
		local input_vmdkid = -1; input_ckptid = -1
		count = 0
		for elm in list.iterator(input_ids) do
			result = split(elm, ":")
			for _,val in ipairs(result) do
				if count == 0 then
					input_vmdkid = tonumber(val)
				elseif count == 1 then
					input_ckptid = tonumber(val)
				end
				count = count + 1
			end

			trace("HYC_UDF input_vmdkid: %d", tonumber(input_vmdkid));
			trace("HYC_UDF input_ckptid: %d", tonumber(input_ckptid));

			if rec_vmdkid == input_vmdkid and rec_ckptid == input_ckptid then
				trace("HYC_UDF found match ckpt ID :: %d", tonumber(rec_ckptid));
				aerospike:remove(rec)
			end
		end
	end
end