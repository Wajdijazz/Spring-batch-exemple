package com.freelance.app.springbatchconfig;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

import com.freelance.app.model.Order;

public class OrderfieldSetMapper implements FieldSetMapper<Order> {

	@Override
	public Order mapFieldSet(FieldSet fieldSet) throws BindException {
		Order order = new Order();
		order.setOrderId(fieldSet.readLong("order_id"));
		order.setCost(fieldSet.readBigDecimal("cost"));
		order.setEmail(fieldSet.readString("email"));
		order.setFirstName(fieldSet.readString("first_name"));
		order.setLastName(fieldSet.readString("last_name"));
		order.setItemId(fieldSet.readString("item_id"));
		order.setItemName(fieldSet.readString("item_name"));
		order.setShipDate(fieldSet.readDate("ship_date"));
		return order;
	}

}
