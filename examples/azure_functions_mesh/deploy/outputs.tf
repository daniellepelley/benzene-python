output "service_urls" {
  description = "Each service Function App's base HTTPS URL (POST {orders url}/orders kicks off the cascade; every service also answers /benzene/invoke|health|spec at the root, per host.json's routePrefix)."
  value       = { for k in local.services : k => "https://${azurerm_linux_function_app.service[k].default_hostname}" }
}

output "service_function_app_names" {
  description = "The six service Function Apps' names (what AzureDiscovery discovers via ARM, and what the mesh's registry.json lists)."
  value       = { for k in local.services : k => azurerm_linux_function_app.service[k].name }
}

output "mesh_function_app_name" {
  description = "The mesh Function App's name (has no HTTP surface — driven by its own Timer trigger; there is no on-demand HTTP invoke)."
  value       = azurerm_linux_function_app.mesh.name
}

output "mesh_ui_url" {
  description = "The static Mesh UI viewer (storage account $web static website), reading the catalog the mesh Function publishes under mesh/."
  value       = "${azurerm_storage_account.this.primary_web_endpoint}mesh/"
}

output "storage_account_name" {
  description = "The storage account backing the Functions runtime, the mesh catalog, and the static viewer."
  value       = azurerm_storage_account.this.name
}
